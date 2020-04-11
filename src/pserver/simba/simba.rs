// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
use crate::pserver::raft::{raft::RaftEngine, state_machine::*};
use crate::pserver::simba::engine::{
    engine::{BaseEngine, Engine},
    faiss::Faiss,
    rocksdb::RocksDB,
    tantivy::Tantivy,
};
use crate::pserver::simba::latch::Latch;
use crate::pserverpb::*;
use crate::sleep;
use crate::util::{
    coding::{doc_key, field_coding, iid_coding, key_coding, slice_slice},
    config,
    entity::*,
    error::*,
    time::current_millis,
};
use log::{error, info, warn};
use prost::Message;
use roaring::RoaringBitmap;
use rocksdb::WriteBatch;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::SeqCst},
    Arc, RwLock,
};
pub struct Simba {
    pub base: Arc<BaseEngine>,
    latch: Latch,
    raft_index: AtomicU64,
    max_iid: AtomicI64,
    del_map: RwLock<RoaringBitmap>,
    //engins
    rocksdb: Arc<RocksDB>,
    tantivy: Tantivy,
    faiss: Faiss,
    raft: Option<Arc<RaftEngine>>,
}

impl Simba {
    pub fn new(
        conf: Arc<config::Config>,
        raft: Option<Arc<RaftEngine>>,
        collection: Arc<Collection>,
        partition: Arc<Partition>,
    ) -> ASResult<Arc<Simba>> {
        let base = Arc::new(BaseEngine {
            conf: conf.clone(),
            collection: collection.clone(),
            partition: partition.clone(),
            stoped: AtomicBool::new(false),
        });

        let rocksdb = RocksDB::new(base.clone())?;
        let tantivy = Tantivy::new(base.clone())?;
        let faiss = Faiss::new(base.clone())?;

        let raft_index = rocksdb.read_raft_index()?;
        let max_iid = rocksdb.find_max_iid();
        let simba = Arc::new(Simba {
            base: base,
            raft: raft,
            latch: Latch::new(50000),
            raft_index: AtomicU64::new(raft_index),
            max_iid: AtomicI64::new(max_iid),
            del_map: RwLock::new(RoaringBitmap::new()),
            rocksdb: Arc::new(rocksdb),
            tantivy: tantivy,
            faiss: faiss,
        });

        let simba_flush = simba.clone();

        std::thread::spawn(move || {
            info!(
                "to start commit job for partition:{} begin",
                simba_flush.base.partition.id
            );
            if let Err(e) = simba_flush.flush() {
                panic!(format!(
                    "flush partition:{} has err :{}",
                    simba_flush.base.partition.id,
                    e.to_string()
                ));
            };
            warn!(
                "parititon:{} stop commit job",
                simba_flush.base.partition.id
            );
        });

        Ok(simba)
    }
    pub fn get(&self, id: &str, sort_key: &str) -> ASResult<Vec<u8>> {
        Ok(self.get_by_key(key_coding(id, sort_key).as_ref())?.1)
    }

    fn get_by_key(&self, key: &Vec<u8>) -> ASResult<(Vec<u8>, Vec<u8>)> {
        let iid = match self.rocksdb.db.get(key) {
            Ok(ov) => match ov {
                Some(v) => v,
                None => return Err(err_code_box(NOT_FOUND, format!("id:{:?} not found!", key))),
            },
            Err(e) => return Err(err_box(format!("get key has err:{}", e.to_string()))),
        };

        match self.rocksdb.db.get(&iid) {
            Ok(ov) => match ov {
                Some(v) => Ok((iid, v)),
                None => Err(err_code_str_box(NOT_FOUND, "iid not found!")),
            },
            Err(e) => Err(err_box(format!("get iid has err:{}", e.to_string()))),
        }
    }

    //it use 1.estimate of rocksdb  2.index of u64
    pub fn count(&self) -> ASResult<(u64, u64)> {
        let estimate_rocksdb = self.rocksdb.count()?;

        let tantivy_count = self.tantivy.count()?;

        Ok((estimate_rocksdb, tantivy_count))
    }

    pub fn search(&self, sdreq: Arc<SearchDocumentRequest>) -> SearchDocumentResponse {
        match self.tantivy.search(sdreq) {
            Ok(r) => r,
            Err(e) => {
                let e = cast_to_err(e);
                SearchDocumentResponse {
                    code: e.0 as i32,
                    total: 0,
                    hits: vec![],
                    info: Some(SearchInfo {
                        error: 1,
                        success: 0,
                        message: format!("search document err:{}", e.1),
                    }),
                }
            }
        }
    }

    pub fn write(&self, req: WriteDocumentRequest, callback: WriteRaftCallback) -> ASResult<()> {
        let (doc, write_type) = (req.doc.unwrap(), WriteType::from_i32(req.write_type));

        match write_type {
            Some(WriteType::Put) => self._put(doc, callback),
            Some(WriteType::Create) => self._create(doc, callback),
            Some(WriteType::Update) => self._update(doc, callback),
            Some(WriteType::Upsert) => self._upsert(doc, callback),
            Some(WriteType::Delete) => self._delete(doc, callback),
            Some(_) | None => {
                return Err(err_box(format!("can not do the handler:{:?}", write_type)));
            }
        }
    }

    fn _create(&self, mut doc: Document, callback: WriteRaftCallback) -> ASResult<()> {
        let key = doc_key(&doc);
        doc.version = 1;
        let mut buf1 = Vec::new();
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }

        let _lock = self.latch.latch_lock(doc.slot);

        if let Err(e) = self.get_by_key(&key) {
            let e = cast_to_err(e);
            if e.0 != NOT_FOUND {
                return Err(e);
            }
        } else {
            return Err(err_box(format!("the document:{:?} already exists", key)));
        }

        self.raft_write(Event::Create(key, buf1), callback)
    }

    fn _update(&self, mut doc: Document, callback: WriteRaftCallback) -> ASResult<()> {
        let (old_version, key) = (doc.version, doc_key(&doc));

        let _lock = self.latch.latch_lock(doc.slot);

        let (old_iid, old) =
            self.get_by_key(&key_coding(doc.id.as_str(), doc.sort_key.as_str()))?;
        let old: Document = Message::decode(prost::bytes::Bytes::from(old))?;
        if old_version > 0 && old.version != old_version {
            return Err(err_code_box(
                VERSION_ERR,
                format!(
                    "the document:{} version not right expected:{} found:{}",
                    doc.id, old_version, old.version
                ),
            ));
        }

        merge_doc(&mut doc, old)?;
        doc.version += old_version + 1;
        let mut buf1 = Vec::new();
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }

        self.raft_write(Event::Update(old_iid, key, buf1), callback)
    }

    fn _upsert(&self, mut doc: Document, callback: WriteRaftCallback) -> ASResult<()> {
        let key = doc_key(&doc);
        let _lock = self.latch.latch_lock(doc.slot);
        let old = match self.get_by_key(key.as_ref()) {
            Ok(o) => Some(o),
            Err(e) => {
                let e = cast_to_err(e);
                if e.0 == NOT_FOUND {
                    None
                } else {
                    return Err(e);
                }
            }
        };

        let event: Event;
        if let Some((iid, old)) = old {
            let old: Document = Message::decode(prost::bytes::Bytes::from(old))?;
            doc.version = old.version + 1;
            merge_doc(&mut doc, old)?;

            let mut buf1 = Vec::new();
            if let Err(error) = doc.encode(&mut buf1) {
                return Err(error.into());
            }

            event = Event::Update(iid, key, buf1);
        } else {
            doc.version = 1;
            let mut buf1 = Vec::new();
            if let Err(error) = doc.encode(&mut buf1) {
                return Err(error.into());
            }
            event = Event::Create(key, buf1);
        }

        self.raft_write(event, callback)
    }

    fn _delete(&self, doc: Document, callback: WriteRaftCallback) -> ASResult<()> {
        let key = doc_key(&doc);
        let _lock = self.latch.latch_lock(doc.slot);

        let iid = match self.rocksdb.db.get(&key) {
            Ok(ov) => match ov {
                Some(v) => v,
                None => return Err(err_code_box(NOT_FOUND, format!("id:{:?} not found!", key))),
            },
            Err(e) => return Err(err_box(format!("get key has err:{}", e.to_string()))),
        };

        self.raft_write(Event::Delete(iid, key), callback)
    }

    fn _put(&self, mut doc: Document, callback: WriteRaftCallback) -> ASResult<()> {
        let key = doc_key(&doc);
        let mut buf1 = Vec::new();
        doc.version = 1;
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }
        let _lock = self.latch.latch_lock(doc.slot);
        let iid = match self.rocksdb.db.get(&key) {
            Ok(iid) => iid,
            Err(_) => None,
        };

        match iid {
            Some(id) => self.raft_write(Event::Update(id, key, buf1), callback),
            None => self.raft_write(Event::Create(key, buf1), callback),
        }
    }

    fn raft_write(&self, event: Event, callback: WriteRaftCallback) -> ASResult<()> {
        self.raft.as_ref().unwrap().append(event, callback)
    }

    pub fn do_write(&self, raft_index: u64, data: &Vec<u8>) -> ASResult<()> {
        let (event, old_iid, key, value) = EventCodec::decode(data);
        if event == EventType::Delete {
            self.rocksdb.delete(key)?;
            self.del_map.write().unwrap().insert(old_iid as u32);
        } else {
            let general_id = self.max_iid.fetch_add(1, SeqCst);
            let iid = iid_coding(general_id);
            let mut batch = WriteBatch::default();
            batch.put(key, &iid)?;
            if self.base.collection.fields.len() == 0 {
                batch.put(iid, value)?;
                return self.rocksdb.write_batch(batch);
            }

            let mut pbdoc: Document = Message::decode(prost::bytes::Bytes::from(value.to_vec()))?;
            let mut source: Value = serde_json::from_slice(pbdoc.source.as_slice())?;
            if self.base.collection.vector_field_index.len() > 0 {
                let map = source.as_object_mut().unwrap();
                for i in self.base.collection.vector_field_index.iter() {
                    let field_name = self.base.collection.fields[*i].name.as_ref().unwrap();
                    if let Some(v) = map.remove(field_name) {
                        let index = self.faiss.get_field(field_name)?;
                        let vector: Vec<f32> = serde_json::from_value(v)?;
                        if index.befor_add(&vector)? {
                            Faiss::start_job(self.rocksdb.clone(), index.clone());
                        }
                        batch.put(field_coding(field_name, general_id), slice_slice(&vector))?;
                    };
                }
                pbdoc.source = serde_json::to_vec(&source)?;
                let mut buf1 = Vec::new();
                if let Err(error) = pbdoc.encode(&mut buf1) {
                    return Err(error.into());
                }
                batch.put(iid, &buf1)?;
            } else {
                batch.put(iid, value)?;
            }
            self.rocksdb.write_batch(batch)?;
            if old_iid > 0 {
                self.del_map.write().unwrap().insert(old_iid as u32);
            }
        }

        self.raft_index.store(raft_index, SeqCst);

        return Ok(());
    }

    pub fn readonly(&self) -> bool {
        return false; //TODO: FIX ME
    }
}

impl Simba {
    fn flush(&self) -> ASResult<()> {
        let flush_time = self.base.conf.ps.flush_sleep_sec.unwrap_or(60) * 1000;

        while !self.base.stoped.load(SeqCst) {
            sleep!(flush_time);

            let begin = current_millis();

            if let Err(e) = self.rocksdb.write_raft_index(self.raft_index.load(SeqCst)) {
                error!("write has err :{:?}", e);
            };
            if let Err(e) = self.rocksdb.flush() {
                error!("rocksdb flush has err:{:?}", e);
            }

            info!("flush job ok use time:{}ms", current_millis() - begin);
        }
        Ok(())
    }

    pub fn stop(&self) {
        self.base.stoped.store(true, SeqCst);
    }

    pub fn release(&self) {
        if !self.base.stoped.load(SeqCst) {
            panic!("call release mut take stop function before");
        }
        self.rocksdb.release();
        self.tantivy.release();

        while Arc::strong_count(&self.rocksdb) > 1 {
            info!(
                "wait release collection:{} partition:{} now is :{}",
                self.base.collection.id.unwrap(),
                self.base.partition.id,
                Arc::strong_count(&self.rocksdb)
            );
            crate::sleep!(300);
        }
    }

    pub fn get_raft_index(&self) -> u64 {
        self.raft_index.load(SeqCst)
    }

    pub fn set_raft_index(&self, raft_index: u64) {
        self.raft_index.store(raft_index, SeqCst);
    }
}

fn merge(a: &mut Value, b: Value) {
    match (a, b) {
        (a @ &mut Value::Object(_), Value::Object(b)) => {
            let a = a.as_object_mut().unwrap();
            for (k, v) in b {
                merge(a.entry(k).or_insert(Value::Null), v);
            }
        }
        (a, b) => *a = b,
    }
}

fn merge_doc(new: &mut Document, old: Document) -> ASResult<()> {
    let mut dist: Value = serde_json::from_slice(new.source.as_slice())?;
    let src: Value = serde_json::from_slice(old.source.as_slice())?;
    merge(&mut dist, src);
    new.source = serde_json::to_vec(&dist)?;
    new.version = old.version + 1;
    Ok(())
}

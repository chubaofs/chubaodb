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
use crate::pserver::raft::*;
#[cfg(not(target_os = "windows"))]
use crate::pserver::simba::engine::faiss::Faiss;
#[cfg(target_os = "windows")]
use crate::pserver::simba::engine::faiss_empty::Faiss;
use crate::pserver::simba::engine::{
    engine::{BaseEngine, Engine},
    rocksdb::RocksDB,
    tantivy::Event as TantivyEvent,
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
use crate::*;
use log::{error, info, warn};
use prost::Message;
use raft4rs::{error::RaftError, raft::Raft};
use roaring::RoaringBitmap;
use rocksdb::WriteBatch;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::SeqCst},
    Arc, RwLock,
};
pub struct Simba {
    pub base: Arc<BaseEngine>,
    latch: Latch,
    raft_index: AtomicU64,
    max_iid: AtomicU32,
    del_map: RwLock<RoaringBitmap>,
    //engins
    rocksdb: Arc<RocksDB>,
    tantivy: Arc<Tantivy>,
    faiss: Faiss,
}

impl Simba {
    pub fn new(
        conf: Arc<config::Config>,
        collection: Arc<Collection>,
        partition: Arc<Partition>,
    ) -> ASResult<Arc<Simba>> {
        let base = Arc::new(BaseEngine {
            conf: conf.clone(),
            collection: collection.clone(),
            partition: partition.clone(),
            stoped: AtomicBool::new(false),
        });

        let rocksdb = Arc::new(RocksDB::new(base.clone())?);
        let tantivy = Tantivy::new(rocksdb.clone(), base.clone())?;
        let faiss = Faiss::new(rocksdb.clone(), base.clone())?;

        let raft_index = rocksdb.read_raft_index()?;
        let max_iid = rocksdb.find_max_iid();
        let simba = Arc::new(Simba {
            base: base,
            latch: Latch::new(50000),
            raft_index: AtomicU64::new(raft_index),
            max_iid: AtomicU32::new(max_iid),
            del_map: RwLock::new(RoaringBitmap::new()),
            rocksdb: rocksdb,
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
        let iid = match self.rocksdb.db.get(key).map_err(cast)? {
            Some(v) => v,
            None => return result!(Code::RocksDBNotFound, "not found id by key:[{:?}]", key),
        };

        match self.rocksdb.get_doc_by_id(&iid).map_err(cast)? {
            Some(v) => Ok((iid, v)),
            None => result!(Code::RocksDBNotFound, "not found iid by key:[{:?}]", &iid),
        }
    }

    pub fn count(&self) -> ASResult<CountDocumentResponse> {
        let mut count_rep = CountDocumentResponse {
            code: Code::Success as i32,
            estimate_count: self.rocksdb.estimate_count()?,
            db_count: self.rocksdb.count()?,
            index_count: self.tantivy.count()?,
            vectors_count: Vec::new(),
            message: String::default(),
        };

        for (name, value) in &self.faiss.fields {
            count_rep.vectors_count.push(VectorCount {
                name: name.clone(),
                count: value.count() as u64,
            });
        }

        Ok(count_rep)
    }

    pub fn search(&self, sdreq: Arc<SearchDocumentRequest>) -> SearchDocumentResponse {
        let mut resp = if sdreq.vector_query.is_none() {
            match self.tantivy.query(sdreq) {
                Ok(r) => r,
                Err(e) => e.into(),
            }
        } else {
            let (bitmap, total) = match self.tantivy.filter(sdreq.clone()) {
                Ok(b) => b,
                Err(e) => return e.into(),
            };
            match self.faiss.search(sdreq, bitmap, total) {
                Ok(r) => r,
                Err(e) => e.into(),
            }
        };

        for hit in resp.hits.iter_mut() {
            match self.rocksdb.get_doc_by_id(&hit.doc) {
                Ok(v) => match v {
                    Some(v) => hit.doc = v,
                    None => error!("not found doc by id :{:?}", &hit.doc),
                },
                Err(e) => return e.into(),
            }
        }

        return resp;
    }

    pub async fn write(&self, req: WriteDocumentRequest, raft: Arc<Raft>) -> ASResult<()> {
        let (doc, write_type) = (req.doc.unwrap(), WriteType::from_i32(req.write_type));
        match write_type {
            Some(WriteType::Put) => self._put(doc, raft).await,
            Some(WriteType::Create) => self._create(doc, raft).await,
            Some(WriteType::Update) => self._update(doc, raft).await,
            Some(WriteType::Upsert) => self._upsert(doc, raft).await,
            Some(WriteType::Delete) => self._delete(doc, raft).await,
            Some(_) | None => {
                return result_def!("can not do the handler:{:?}", write_type);
            }
        }
    }

    async fn _create(&self, mut doc: Document, raft: Arc<Raft>) -> ASResult<()> {
        let key = doc_key(&doc);
        doc.version = 1;
        let buf1 = self.doc_encoding(&mut doc)?;

        let _lock = self.latch.latch_lock(doc.slot);

        if let Err(e) = self.get_by_key(&key) {
            if e.code() != Code::RocksDBNotFound {
                return Err(e);
            }
        } else {
            return result_def!("the document:{:?} already exists", key);
        }

        self.raft_write(Event::Create(key, buf1), raft).await
    }

    async fn _update(&self, mut doc: Document, raft: Arc<Raft>) -> ASResult<()> {
        let (old_version, key) = (doc.version, doc_key(&doc));

        let _lock = self.latch.latch_lock(doc.slot);

        let (old_iid, old) =
            self.get_by_key(&key_coding(doc.id.as_str(), doc.sort_key.as_str()))?;
        let old: Document = Message::decode(prost::bytes::Bytes::from(old))?;
        if old_version > 0 && old.version != old_version {
            return result!(
                Code::VersionErr,
                "the document:{} version not right expected:{} found:{}",
                doc.id,
                old_version,
                old.version
            );
        }

        merge_doc(&mut doc, old)?;
        doc.version += old_version + 1;

        let buf1 = self.doc_encoding(&mut doc)?;
        self.raft_write(Event::Update(old_iid, key, buf1), raft)
            .await
    }

    async fn _upsert(&self, mut doc: Document, raft: Arc<Raft>) -> ASResult<()> {
        let key = doc_key(&doc);
        let _lock = self.latch.latch_lock(doc.slot);
        let old = match self.get_by_key(key.as_ref()) {
            Ok(o) => Some(o),
            Err(e) => {
                if e.code() == Code::RocksDBNotFound {
                    None
                } else {
                    return Err(e);
                }
            }
        };

        let event: Event;
        if let Some((iid, old)) = old {
            let old: Document = Message::decode(prost::bytes::Bytes::from(old))?;
            merge_doc(&mut doc, old)?;
            doc.version += 1;
            let buf1 = self.doc_encoding(&mut doc)?;

            event = Event::Update(iid, key, buf1);
        } else {
            doc.version = 1;
            let buf1 = self.doc_encoding(&mut doc)?;
            event = Event::Create(key, buf1);
        }

        self.raft_write(event, raft).await
    }

    async fn _delete(&self, doc: Document, raft: Arc<Raft>) -> ASResult<()> {
        let key = doc_key(&doc);
        let _lock = self.latch.latch_lock(doc.slot);

        let iid = match self.rocksdb.db.get(&key) {
            Ok(ov) => match ov {
                Some(v) => v,
                None => return result!(Code::RocksDBNotFound, "id:{:?} not found!", key,),
            },
            Err(e) => return result_def!("get key has err:{}", e),
        };

        self.raft_write(Event::Delete(iid, key), raft).await
    }

    async fn _put(&self, mut doc: Document, raft: Arc<Raft>) -> ASResult<()> {
        let key = doc_key(&doc);
        doc.version = 1;
        let buf1 = self.doc_encoding(&mut doc)?;
        let _lock = self.latch.latch_lock(doc.slot);
        let iid = match self.rocksdb.db.get(&key) {
            Ok(iid) => iid,
            Err(_) => None,
        };

        match iid {
            Some(id) => self.raft_write(Event::Update(id, key, buf1), raft).await,
            None => self.raft_write(Event::Create(key, buf1), raft).await,
        }
    }

    pub fn doc_encoding(&self, doc: &mut Document) -> ASResult<Vec<u8>> {
        let mut buf = Vec::new();
        if self.base.collection.fields.len() == 0 {
            if let Err(error) = doc.encode(&mut buf) {
                return Err(error.into());
            }
            return Ok(buf);
        }

        let mut source: Value = serde_json::from_slice(doc.source.as_slice())?;

        for i in self.base.collection.scalar_field_index.iter() {
            let field = &self.base.collection.fields[*i];
            field.validate(source.get(field.name()))?;
        }

        if self.base.collection.vector_field_index.len() == 0 {
            if let Err(error) = doc.encode(&mut buf) {
                return Err(error.into());
            }
            return Ok(buf);
        }

        let map = source.as_object_mut().unwrap();

        let mut vectors = Vec::with_capacity(self.base.collection.vector_field_index.len());

        for i in self.base.collection.vector_field_index.iter() {
            let field = match &self.base.collection.fields[*i] {
                Field::vector(field) => field,
                _ => panic!(format!("vector field index has not field index:{}", *i)),
            };
            let value = field.validate(map.remove(&field.name))?;
            if value.len() == 0 {
                continue;
            }

            vectors.push(Vector {
                name: field.name.clone(),
                vector: value,
            });
        }

        doc.vectors = vectors;
        doc.source = serde_json::to_vec(&source)?;

        if let Err(error) = doc.encode(&mut buf) {
            return Err(error.into());
        }
        return Ok(buf);
    }

    async fn raft_write(&self, event: Event, raft: Arc<Raft>) -> ASResult<()> {
        match raft.submit(event.encode()).await {
            Ok(()) => Ok(()),
            Err(e) => match e {
                RaftError::ErrCode(c, m) => {
                    println!(
                        "111======================================{}",
                        ASError::Error(Code::from_i32(c), m.clone())
                    );
                    Err(ASError::Error(Code::from_i32(c), m))
                }
                _ => {
                    println!("======================================{}", e);
                    Err(ASError::from(e))
                }
            },
        }
    }

    pub fn do_write(&self, raft_index: u64, data: &[u8], check: bool) -> ASResult<()> {
        let (event, old_iid, key, value) = Event::decode(data);

        if event == EventType::Delete {
            self.rocksdb.delete(key)?;
            self.tantivy.write(TantivyEvent::Delete(old_iid))?;
            self.del_map.write().unwrap().insert(old_iid as u32);
        } else {
            let general_id = self.general_id();
            let iid = iid_coding(general_id);
            let mut batch = WriteBatch::default();
            batch.put(key, &iid);
            if self.base.collection.fields.len() == 0 {
                batch.put_cf(self.rocksdb.id_cf(), iid, value);
                return self.rocksdb.write_batch(batch);
            }

            if self.base.collection.vector_field_index.len() > 0 {
                let mut pbdoc: Document =
                    Message::decode(prost::bytes::Bytes::from(value.to_vec()))?;

                let vectors = pbdoc.vectors;
                pbdoc.vectors = Vec::new();
                for v in vectors {
                    batch.put(field_coding(&v.name, general_id), slice_slice(&v.vector));
                }

                let mut buf1 = Vec::new();
                if let Err(error) = pbdoc.encode(&mut buf1) {
                    return Err(ASError::Error(Code::EncodingErr, error.to_string()));
                };

                batch.put_cf(self.rocksdb.id_cf(), iid, &buf1);
            } else {
                batch.put_cf(self.rocksdb.id_cf(), iid, value);
            }

            if !check
                || self
                    .rocksdb
                    .get_doc_by_id(&iid_coding(general_id))?
                    .is_none()
            {
                self.rocksdb.write_batch(batch)?;
            }

            if !check || !self.tantivy.exist(general_id)? {
                self.tantivy
                    .write(TantivyEvent::Update(old_iid, general_id))?;
                if old_iid > 0 {
                    self.del_map.write().unwrap().insert(old_iid);
                }
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
        let flush_time = self.base.conf.ps.flush_sleep_sec.unwrap_or(3) * 1000;

        let mut times = 0;

        let mut pre_index = 0;

        while !self.base.stoped.load(SeqCst) {
            times += 1;

            sleep!(flush_time);

            let begin = current_millis();

            let index = self.raft_index.load(SeqCst);

            if let Err(e) = self.tantivy.flush() {
                error!("flush tantivy has err :{:?}", e);
            };

            if let Err(e) = self.faiss.flush() {
                error!("flush faiss has err :{:?}", e);
            };

            if times % 10 == 0 && pre_index < index {
                if let Err(e) = self.rocksdb.write_raft_index(pre_index) {
                    error!("write has err :{:?}", e);
                };
                if let Err(e) = self.rocksdb.flush() {
                    error!("rocksdb flush has err:{:?}", e);
                }
                pre_index = index;
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

        while Arc::strong_count(&self.rocksdb) > self.rocksdb.arc_count.load(SeqCst) as usize {
            info!(
                "wait release collection:{} partition:{} now is :{}/{}",
                self.base.collection.id,
                self.base.partition.id,
                Arc::strong_count(&self.rocksdb),
                self.rocksdb.arc_count.load(SeqCst)
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

    pub fn general_id(&self) -> u32 {
        let id = self.max_iid.fetch_add(1, SeqCst);
        return id + 1;
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
    let new_src: Value = serde_json::from_slice(new.source.as_slice())?;
    let mut old_src: Value = serde_json::from_slice(old.source.as_slice())?;
    merge(&mut old_src, new_src);
    new.source = serde_json::to_vec(&old_src)?;
    new.version = old.version;
    Ok(())
}

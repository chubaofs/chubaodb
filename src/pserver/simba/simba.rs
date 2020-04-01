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
use rocksdb::WriteBatch;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc, RwLock,
};

pub struct Simba {
    pub base: Arc<BaseEngine>,
    readonly: bool,
    stoped: AtomicBool,
    latch: Latch,
    max_sn: RwLock<u64>,
    //engins
    rocksdb: Arc<RocksDB>,
    tantivy: Tantivy,
    faiss: Faiss,
}

impl Simba {
    pub fn new(
        conf: Arc<config::Config>,
        readonly: bool,
        collection: Arc<Collection>,
        partition: Arc<Partition>,
    ) -> ASResult<Arc<Simba>> {
        let base = Arc::new(BaseEngine {
            conf: conf.clone(),
            collection: collection.clone(),
            partition: partition.clone(),
        });

        let rocksdb = RocksDB::new(base.clone())?;
        let tantivy = Tantivy::new(base.clone())?;
        let faiss = Faiss::new(base.clone())?;

        let sn = rocksdb.read_sn()?;
        let simba = Arc::new(Simba {
            base: base,
            readonly: readonly,
            stoped: AtomicBool::new(false),
            latch: Latch::new(50000),
            max_sn: RwLock::new(sn),
            rocksdb: Arc::new(rocksdb),
            tantivy: tantivy,
            faiss: faiss,
        });

        let simba_flush = simba.clone();

        tokio::spawn(async move {
            if readonly {
                return;
            }
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
        self.get_by_key(key_coding(id, sort_key).as_ref())
    }

    fn get_by_key(&self, key: &Vec<u8>) -> ASResult<Vec<u8>> {
        let iid = match self.rocksdb.db.get(key) {
            Ok(ov) => match ov {
                Some(v) => v,
                None => return Err(err_code_box(NOT_FOUND, format!("id:{:?} not found!", key))),
            },
            Err(e) => return Err(err_box(format!("get key has err:{}", e.to_string()))),
        };

        match self.rocksdb.db.get(iid) {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
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

    pub async fn write(&self, req: WriteDocumentRequest) -> ASResult<()> {
        let (doc, write_type) = (req.doc.unwrap(), WriteType::from_i32(req.write_type));

        match write_type {
            Some(WriteType::Overwrite) => self._overwrite(doc).await,
            Some(WriteType::Create) => self._overwrite(doc).await,
            Some(WriteType::Update) => self._update(doc).await,
            Some(WriteType::Upsert) => self._upsert(doc).await,
            Some(WriteType::Delete) => self._delete(doc).await,
            Some(_) | None => {
                return Err(err_box(format!("can not do the handler:{:?}", write_type)));
            }
        }
    }

    async fn _create(&self, mut doc: Document) -> ASResult<()> {
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

        self.do_write(&key, &buf1).await
    }

    async fn _update(&self, mut doc: Document) -> ASResult<()> {
        let (old_version, key) = (doc.version, doc_key(&doc));

        let _lock = self.latch.latch_lock(doc.slot);
        let old = self.get(doc.id.as_str(), doc.sort_key.as_str())?;
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

        self.do_write(&key, &buf1).await
    }

    async fn _upsert(&self, mut doc: Document) -> ASResult<()> {
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

        if let Some(old) = old {
            let old: Document = Message::decode(prost::bytes::Bytes::from(old))?;
            doc.version = old.version + 1;
            merge_doc(&mut doc, old)?;
        } else {
            doc.version = 1;
        }

        let mut buf1 = Vec::new();
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }
        self.do_write(&key, &buf1).await
    }

    async fn _delete(&self, doc: Document) -> ASResult<()> {
        let key = doc_key(&doc);
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_delete(&key).await
    }

    async fn _overwrite(&self, mut doc: Document) -> ASResult<()> {
        let key = doc_key(&doc);
        let mut buf1 = Vec::new();
        doc.version = 1;
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_write(&key, &buf1).await
    }

    async fn do_write(&self, key: &Vec<u8>, value: &Vec<u8>) -> ASResult<()> {
        //general iid .......
        let general_id = 123;

        let iid = iid_coding(general_id);

        let mut batch = WriteBatch::default();
        batch.put(key, &iid)?;

        if self.base.collection.fields.len() == 0 {
            batch.put(iid, value)?;
            return self.rocksdb.write_batch(batch);
        }

        let mut pbdoc: Document = Message::decode(prost::bytes::Bytes::from(value.to_vec()))?;
        let mut source: Value = serde_json::from_slice(pbdoc.source.as_slice())?;

        if self.base.collection.vector {
            let map = source.as_object_mut().unwrap();
            for f in self.base.collection.fields.iter() {
                match f.internal_type {
                    FieldType::VECTOR => {
                        let field_name: &str = f.name.as_ref().unwrap();
                        if let Some(v) = map.remove(field_name) {
                            let index = self.faiss.get_field(field_name)?;
                            let vector: Vec<f32> = serde_json::from_value(v)?;
                            if index.validate(&vector)? {
                                //start train job.....
                            }
                            batch
                                .put(field_coding(general_id, field_name), slice_slice(&vector))?;
                        };
                    }
                    _ => {}
                }
            }
            pbdoc.source = serde_json::to_vec(&source)?;
        }

        batch.put(iid, value)?;
        self.rocksdb.write_batch(batch)?;

        Ok(())
    }

    async fn do_delete(&self, key: &Vec<u8>) -> ASResult<()> {
        self.rocksdb.delete(key)?;
        self.tantivy.delete(key)?;
        Ok(())
    }

    pub fn readonly(&self) -> bool {
        return self.readonly;
    }
}

impl Simba {
    fn flush(&self) -> ASResult<()> {
        let flush_time = self.base.conf.ps.flush_sleep_sec.unwrap_or(3) * 1000;

        let mut pre_sn = self.get_sn();

        while !self.stoped.load(SeqCst) {
            sleep!(flush_time);

            let sn = self.get_sn();

            //TODO: check pre_sn < current sn , and set

            let begin = current_millis();

            if let Err(e) = self.rocksdb.flush() {
                error!("rocksdb flush has err:{:?}", e);
            }

            if let Err(e) = self.tantivy.flush() {
                error!("rocksdb flush has err:{:?}", e);
            }

            pre_sn = sn;

            if let Err(e) = self.rocksdb.write_sn(pre_sn) {
                error!("write has err :{:?}", e);
            };

            info!("flush job ok use time:{}ms", current_millis() - begin);
        }
        Ok(())
    }

    pub fn stop(&self) {
        self.stoped.store(true, SeqCst);
    }

    pub fn release(&self) {
        if !self.stoped.load(SeqCst) {
            panic!("call release mut take stop function before");
        }
        self.rocksdb.release();
        self.tantivy.release();
    }

    pub fn get_sn(&self) -> u64 {
        *self.max_sn.read().unwrap()
    }

    pub fn set_sn_if_max(&self, sn: u64) {
        let mut v = self.max_sn.write().unwrap();
        if *v < sn {
            *v = sn;
        }
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

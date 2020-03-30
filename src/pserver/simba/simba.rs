// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.
use crate::pserver::simba::engine::{
    engine::{BaseEngine, Engine},
    raft::*,
    rocksdb::RocksDB,
    tantivy::Tantivy,
};
use crate::pserver::simba::latch::Latch;
use crate::pserverpb::*;
use crate::sleep;
use crate::util::{
    coding::{doc_id, id_coding},
    config::*,
    entity::*,
    error::*,
};
use fp_rust::sync::CountDownLatch;
use log::{error, info, warn};
use prost::Message;
use serde_json::Value;
use std::cmp;
use std::marker::Send;
use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc, RwLock,
};

pub struct Simba {
    pub conf: Arc<Config>,
    _collection: Arc<Collection>,
    pub partition: Arc<Partition>,
    readonly: bool,
    pub started: AtomicBool,
    writable: AtomicBool,
    latch: Latch,
    //engins
    pub rocksdb: Option<RocksDB>,
    pub tantivy: Option<Tantivy>,
    pub raft: Option<RaftEngine>,
    pub base_engine: Arc<BaseEngine>,
    pub server_id: u64,
    pub start_latch: Arc<Option<CountDownLatch>>,
}

unsafe impl Send for Simba {}

impl Simba {
    pub fn new(
        conf: Arc<Config>,
        readonly: bool,
        collection: Arc<Collection>,
        partition: Arc<Partition>,
        server_id: u64,
        latch: Arc<Option<CountDownLatch>>,
    ) -> ASResult<Arc<RwLock<Simba>>> {
        let base: Arc<BaseEngine> = Arc::new(BaseEngine {
            conf: conf.clone(),
            collection: collection.clone(),
            partition: partition.clone(),
            max_sn: RwLock::new(0),
        });
        let simba: Arc<RwLock<Simba>> = Arc::new(RwLock::new(Simba {
            rocksdb: None,
            tantivy: None,
            raft: None,
            conf: conf.clone(),
            _collection: collection.clone(),
            partition: partition.clone(),
            readonly: readonly,
            started: AtomicBool::new(true),
            writable: AtomicBool::new(false),
            latch: Latch::new(50000),
            base_engine: base.clone(),
            server_id: server_id,
            start_latch: latch.clone(),
        }));

        let raft: RaftEngine = RaftEngine::new(base.clone(), simba.clone());
        simba.write().unwrap().raft = Some(raft);
        let simba_flush = simba.clone();

        tokio::spawn(async move {
            if readonly {
                return;
            }
            info!(
                "to start commit job for partition:{} begin",
                simba_flush.read().unwrap().partition.id
            );
            if let Err(e) = simba_flush.read().unwrap().flush() {
                panic!(format!(
                    "flush partition:{} has err :{}",
                    simba_flush.read().unwrap().partition.id,
                    e.to_string()
                ));
            };
            warn!(
                "parititon:{} stop commit job",
                simba_flush.read().unwrap().partition.id
            );
        });

        Ok(simba.clone())
    }

    pub fn get(&self, id: &str, sort_key: &str) -> ASResult<Vec<u8>> {
        self.get_by_iid(id_coding(id, sort_key).as_ref())
    }

    fn get_by_iid(&self, iid: &Vec<u8>) -> ASResult<Vec<u8>> {
        match self.rocksdb.unwrap().db.get(iid) {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
                None => Err(err_code_str_box(NOT_FOUND, "not found!")),
            },
            Err(e) => Err(err_box(format!("get key has err:{}", e.to_string()))),
        }
    }

    //it use estimate
    pub fn count(&self) -> ASResult<u64> {
        match self
            .rocksdb
            .unwrap()
            .db
            .property_int_value("rocksdb.estimate-num-keys")
        {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
                None => Ok(0),
            },
            Err(e) => Err(err_box(format!("{}", e.to_string()))),
        }
    }

    pub fn search(&self, sdreq: Arc<SearchDocumentRequest>) -> SearchDocumentResponse {
        match self.tantivy.unwrap().search(sdreq) {
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

    pub fn write(&self, req: WriteDocumentRequest) -> ASResult<()> {
        let (doc, write_type) = (req.doc.unwrap(), WriteType::from_i32(req.write_type));

        match write_type {
            Some(WriteType::Overwrite) => self._overwrite(doc),
            Some(WriteType::Create) => self._overwrite(doc),
            Some(WriteType::Update) => self._update(doc),
            Some(WriteType::Upsert) => self._upsert(doc),
            Some(WriteType::Delete) => self._delete(doc),
            Some(_) | None => {
                return Err(err_box(format!("can not do the handler:{:?}", write_type)));
            }
        }
    }

    fn _create(&self, mut doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        doc.version = 1;
        let mut buf1 = Vec::new();
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }

        let _lock = self.latch.latch_lock(doc.slot);

        if let Err(e) = self.get_by_iid(&iid) {
            let e = cast_to_err(e);
            if e.0 != NOT_FOUND {
                return Err(e);
            }
        } else {
            return Err(err_box(format!("the document:{:?} already exists", iid)));
        }

        self.do_write(&iid, &buf1)
    }

    fn _update(&self, mut doc: Document) -> ASResult<()> {
        let (old_version, iid) = (doc.version, doc_id(&doc));

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

        self.do_write(&iid, &buf1)
    }

    fn _upsert(&self, mut doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let _lock = self.latch.latch_lock(doc.slot);
        let old = match self.get_by_iid(iid.as_ref()) {
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
        self.do_write(&iid, &buf1)
    }

    fn _delete(&self, doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_delete(&iid)
    }

    fn _overwrite(&self, mut doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let mut buf1 = Vec::new();
        doc.version = 1;
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_write(&iid, &buf1)
    }

    fn do_write(&self, key: &Vec<u8>, value: &Vec<u8>) -> ASResult<()> {
        if self.check_writable() {
            //get raft sn
            let sn: u64 = self.raft.as_ref().unwrap().get_sn();
            self.rocksdb.as_ref().unwrap().write(sn, key, value)?;
            self.tantivy.as_ref().unwrap().write(sn, key, value)?;
            let latch = CountDownLatch::new(1);
            self.raft.as_ref().unwrap().append(
                PutEvent {
                    k: key.to_vec(),
                    v: value.to_vec(),
                },
                WriteRaftCallback { latch: latch },
            );
            latch.wait();
            Ok(())
        } else {
            Err(err_code_str_box(ENGINE_NOT_WRITABLE, "engin not writable!"))
        }
    }

    async fn do_delete(&self, key: &Vec<u8>) -> ASResult<()> {
        if self.check_writable() {
            let mut sn: u64 = self.raft.as_ref().unwrap().get_sn();
            if sn == 0 {
                sn = cmp::min(
                    self.rocksdb.as_ref().unwrap().get_sn(),
                    self.tantivy.as_ref().unwrap().get_sn(),
                );
            }
            self.rocksdb.as_ref().unwrap().delete(sn, key)?;
            self.tantivy.as_ref().unwrap().delete(sn, key)?;
            let latch = CountDownLatch::new(1);
            self.raft.as_ref().unwrap().append(
                DelEvent { k: key.to_vec() },
                WriteRaftCallback { latch: latch },
            );
            latch.wait();

            Ok(())
        } else {
            Err(err_code_str_box(ENGINE_NOT_WRITABLE, "engin not writable!"))
        }
    }

    pub fn readonly(&self) -> bool {
        return self.readonly;
    }
}

pub struct WriteRaftCallback {
    pub latch: CountDownLatch,
}
impl AppendCallback for WriteRaftCallback {
    fn call(&self) {
        self.latch.countdown();
    }
}

impl Simba {
    fn flush(&self) -> ASResult<()> {
        let flush_time = self.conf.ps.flush_sleep_sec.unwrap_or(3) * 1000;

        let mut pre_db_sn = self.rocksdb.as_ref().unwrap().get_sn();
        let mut pre_tantivy_sn = self.rocksdb.as_ref().unwrap().get_sn();
        loop {
            if self.check_writable() {
                sleep!(flush_time);
                let mut flag = false;
                if let Some(sn) = self.rocksdb.as_ref().unwrap().flush(pre_db_sn) {
                    pre_db_sn = sn;
                    flag = true;
                }
                if let Some(sn) = self.tantivy.as_ref().unwrap().flush(pre_tantivy_sn) {
                    pre_tantivy_sn = sn;
                    flag = true;
                }
                if flag {
                    if let Err(e) = self
                        .rocksdb
                        .as_ref()
                        .unwrap()
                        .write_sn(pre_db_sn, pre_tantivy_sn)
                    {
                        error!("write has err :{:?}", e);
                    };
                }
            }
            sleep!(flush_time);
        }
        Ok(())
    }

    pub fn release(&self) {
        self.started.store(false, SeqCst);
        self.raft.as_ref().unwrap().release();
        self.offload_engine();
    }

    pub fn role_change(self, is_leader: bool) {
        if self.started.load(SeqCst) {
            let _lock = self.latch.latch_lock(u32::max_value());

            if is_leader {
                self.load_engine();
            } else {
                self.offload_engine();
            }
            if self.start_latch.is_some() {
                self.start_latch.as_ref().unwrap().countdown();
            }
        }
    }
    fn offload_engine(&self) {
        self.writable.store(false, SeqCst);
        self.rocksdb.as_ref().unwrap().release();
        self.tantivy.as_ref().unwrap().release();
    }

    fn load_engine(&self) {
        let rocksdb = RocksDB::new(BaseEngine::new(&self.base_engine)).unwrap();
        let tantivy = Tantivy::new(BaseEngine::new(&self.base_engine)).unwrap();
        let log_start_index = cmp::min(rocksdb.get_sn(), tantivy.get_sn());

        //TODO load log and restore
        //self.raft.unwrap().read();
        let data: Vec<u8> = vec![0, 0, 0, 0];
        let log_index = 0;
        if data.len() > 0 {
            match LogEvent::to_event(data) {
                Ok(event) => match event.get_type() {
                    EventType::Delete => {
                        let del = DelEvent {
                            k: vec![0, 0, 0, 0],
                        }; //event as Box<DelEvent>;
                        rocksdb.delete(log_index, &del.k.clone());
                        tantivy.delete(log_index, &del.k.clone());
                    }
                    EventType::Put => {
                        // let put = event as Box<PutEvent>;
                        // rocksdb.write(log_index, &put.k.clone(), &put.v.clone());
                        // tantivy.write(log_index, &put.k.clone(), &put.v.clone());
                    }
                },
                Err(e) => print!("error"),
            }
        }
        self.rocksdb = Some(rocksdb);
        self.tantivy = Some(tantivy);
        self.writable.store(true, SeqCst);
    }

    pub fn check_writable(&self) -> bool {
        self.started.load(SeqCst) && !self.writable.load(SeqCst)
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

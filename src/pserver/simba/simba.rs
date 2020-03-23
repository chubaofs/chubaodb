// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.
use crate::pserver::simba::indexer::Indexer;
use crate::pserver::simba::latch::Latch;
use crate::pserverpb::*;
use crate::sleep;
use crate::util::{
    coding::{doc_id, id_coding,u64_slice, slice_u64},
    config,
    entity::*,
    error::*,
};
use log::{error, info, warn};
use prost::Message;
use rocksdb::{FlushOptions, WriteBatch, WriteOptions, DB};
use serde_json::Value;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    Arc, RwLock,
};
use std::thread;

const SYSTEM_CF: &'static str = "_system";

pub trait Engine {
    fn get_sn(&self) -> u64;
    fn set_sn_if_max(&self, sn: u64);
    fn flush(&self) -> ASResult<()>;
    fn release(&self);
}

pub struct Simba {
    conf: Arc<config::Config>,
    pub collection_id: u32,
    pub partition: Partition,
    readonly: bool,
    stoped: AtomicBool,
    db: Arc<DB>,
    system_db: DB,
    indexer: Arc<Indexer>,
    latch: Latch,
    max_sn: RwLock<u64>,
}

impl Simba {
    pub fn new(
        conf: Arc<config::Config>,
        readonly: bool,
        collection: &Collection,
        partition: &Partition,
    ) -> ASResult<Arc<Simba>> {
        let base_path = Path::new(&conf.ps.data)
            .join(Path::new(format!("{}", partition.collection_id).as_str()))
            .join(Path::new(format!("{}", partition.id).as_str()));

        let db_path = base_path.join(Path::new("db"));

        let mut option = rocksdb::Options::default();
        option.create_if_missing(true);
        option.set_max_background_flushes(16);
        option.increase_parallelism(16);

        let mut db = DB::open(&option, db_path.to_str().unwrap())?;

        db.create_cf(SYSTEM_CF, &option)?; //TODO: has errr??????????????????

        let system_db = DB::open_cf(&option, db_path.to_str().unwrap(), &[SYSTEM_CF])? ;

        let db = Arc::new(db);

        let indexer = Indexer::new(conf.clone(), base_path.clone(), collection, partition)?;

        let simba = Arc::new(Simba {
            conf: conf.clone(),
            collection_id: partition.collection_id,
            partition: partition.clone(),
            readonly: readonly,
            stoped: AtomicBool::new(false),
            db: db,
            system_db:system_db,
            indexer: indexer,
            latch: Latch::new(50000),
            max_sn: RwLock::new(0),
        });

        let simba_flush = simba.clone();

        tokio::spawn(async move {
            if readonly {
                return;
            }
            info!(
                "to start commit job for partition:{} begin",
                simba_flush.partition.id
            );
            if let Err(e) = simba_flush.flush() {
                panic!(format!(
                    "flush partition:{} has err :{}",
                    simba_flush.partition.id,
                    e.to_string()
                ));
            };
            warn!("parititon:{} stop commit job", simba_flush.partition.id);
        });

        Ok(simba)
    }
    pub fn get(&self, id: &str, sort_key: &str) -> ASResult<Vec<u8>> {
        self.get_by_iid(id_coding(id, sort_key).as_ref())
    }

    fn get_by_iid(&self, iid: &Vec<u8>) -> ASResult<Vec<u8>> {
        match self.db.get(iid) {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
                None => Err(err_code_str_box(NOT_FOUND, "not found!")),
            },
            Err(e) => Err(err_box(format!("get key has err:{}", e.to_string()))),
        }
    }

    //it use estimate
    pub fn count(&self) -> ASResult<u64> {
        match self.db.property_int_value("rocksdb.estimate-num-keys") {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
                None => Ok(0),
            },
            Err(e) => Err(err_box(format!("{}", e.to_string()))),
        }
    }

    pub fn search(&self, sdreq: Arc<SearchDocumentRequest>) -> SearchDocumentResponse {
        match self.indexer.search(sdreq) {
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

        self.do_write(&iid, &buf1).await
    }

    async fn _update(&self, mut doc: Document) -> ASResult<()> {
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

        self.do_write(&iid, &buf1).await
    }

    async fn _upsert(&self, mut doc: Document) -> ASResult<()> {
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
        self.do_write(&iid, &buf1).await
    }

    async fn _delete(&self, doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_delete(&iid).await
    }

    async fn _overwrite(&self, mut doc: Document) -> ASResult<()> {
        let iid = doc_id(&doc);
        let mut buf1 = Vec::new();
        doc.version = 1;
        if let Err(error) = doc.encode(&mut buf1) {
            return Err(error.into());
        }
        let _lock = self.latch.latch_lock(doc.slot);
        self.do_write(&iid, &buf1).await
    }

    async fn do_write(&self, key: &Vec<u8>, value: &Vec<u8>) -> ASResult<()> {
        let sn: u64 = 11111; //TODO: get raft sn
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        write_options.set_sync(false);
        let mut batch = WriteBatch::default();
        batch.put(key, value)?;
        convert(self.db.write_opt(batch, &write_options))?;
        self.indexer.write(key, value)?;
        self.set_sn_if_max(sn);
        Ok(())
    }

    async fn do_delete(&self, key: &Vec<u8>) -> ASResult<()> {
        let sn: u64 = 11111; //TODO: get raft sn
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        write_options.set_sync(false);
        let mut batch = WriteBatch::default();
        batch.delete(key)?;
        convert(self.db.write_opt(batch, &write_options))?;
        self.indexer.delete(key)?;
        self.set_sn_if_max(sn);
        Ok(())
    }

    pub fn readonly(&self) -> bool {
        return self.readonly;
    }

    pub fn write_sn(&self, db_sn: u64, indexer_sn: u64) ->ASResult<()> {
        let write_options = WriteOptions::default();
        let mut batch = WriteBatch::default();
        batch.put(b"db_sn", &u64_slice(db_sn)[..])?;
        batch.put(b"indexer_sn", &u64_slice(indexer_sn)[..])?;
        convert(self.system_db.write_opt(batch, &write_options))?;
        Ok(())
    }
}

impl Engine for Simba {
    fn get_sn(&self) -> u64 {
        *self.max_sn.read().unwrap()
    }

    fn set_sn_if_max(&self, sn: u64) {
        let mut v = self.max_sn.write().unwrap();
        if *v < sn {
            *v = sn;
        }
    }

    fn flush(&self) -> ASResult<()> {
        let flush_time = self.conf.ps.flush_sleep_sec.unwrap_or(3) * 1000;

        let mut pre_db_sn = self.get_sn();
        let mut pre_indexer_sn = self.indexer.get_sn();

        while !self.stoped.load(SeqCst) {
            sleep!(flush_time);

            let mut flag = false;

            let db_sn = self.get_sn();
            if pre_db_sn < db_sn {
                let mut flush_options = FlushOptions::default();
                flush_options.set_wait(false);
                if let Err(e) = self.db.flush_opt(&flush_options) {
                    error!("flush db has err :{:?}", e);
                }
                pre_db_sn = db_sn;
                flag = true;
            }

            let indexer_sn = self.indexer.get_sn();
            if pre_indexer_sn < indexer_sn {
                if let Err(e) = self.indexer.flush() {
                    error!("flush indexer has err :{:?}", e);
                }
                pre_indexer_sn = indexer_sn;
                flag = true;
            }

            if flag{
                if let Err(e) = self.write_sn(pre_db_sn, pre_indexer_sn){
                    error!("write has err :{:?}", e);
                } ;
            }
        }
        Ok(())
    }

    fn release(&self) {
        self.stoped.store(true, SeqCst);
        let mut flush_options = FlushOptions::default();
        flush_options.set_wait(true);
        if let Err(e) = self.db.flush_opt(&flush_options) {
            error!("flush db has err:{:?}", e);
        }

        while Arc::strong_count(&self.db) > 1 {
            info!(
                "wait release rocksdb collection:{} partition:{} now is :{}",
                self.collection_id,
                self.partition.id,
                Arc::strong_count(&self.db)
            );
            thread::sleep(std::time::Duration::from_millis(300));
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

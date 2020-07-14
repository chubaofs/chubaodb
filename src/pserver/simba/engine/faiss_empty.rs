use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::pserver::simba::engine::rocksdb::RocksDB;
use crate::pserverpb::*;
use crate::util::error::*;
use crate::*;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
#[derive(PartialEq, Copy, Clone)]
pub enum IndexStatus {
    NotReady,
    Runging,
    Stoping,
    Stoped,
}

// this struct for to use without faiss example  window or user not need vector search
pub struct Faiss {
    _db: Arc<RocksDB>,
    base: Arc<BaseEngine>,
    pub fields: HashMap<String, Arc<IndexField>>,
}

impl Deref for Faiss {
    type Target = Arc<BaseEngine>;
    fn deref<'a>(&'a self) -> &'a Arc<BaseEngine> {
        &self.base
    }
}

pub struct IndexField {}

impl IndexField {
    pub fn count(&self) -> u32 {
        0
    }
}

impl Faiss {
    pub fn new(db: Arc<RocksDB>, base: Arc<BaseEngine>) -> ASResult<Faiss> {
        Ok(Faiss {
            _db: db,
            base: base,
            fields: HashMap::new(),
        })
    }

    pub fn search(
        &self,
        _sdreq: Arc<QueryRequest>,
        _bitmap: Option<RoaringBitmap>,
        _total: u64,
    ) -> ASResult<SearchDocumentResponse> {
        return result_def!("not support");
    }

    pub fn get_field(&self, _name: &str) -> ASResult<Arc<IndexField>> {
        return result_def!("not support");
    }
}

impl Engine for Faiss {
    fn flush(&self) -> ASResult<()> {
        Ok(())
    }

    fn release(&self) {}
}

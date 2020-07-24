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
use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::util::{
    coding::{slice_u32, slice_u64, u64_slice, RAFT_INDEX_KEY},
    error::*,
};
use crate::*;
use log::{error, info};
use rocksdb::{ColumnFamily, Direction, FlushOptions, IteratorMode, WriteBatch, WriteOptions, DB};
use std::ops::Deref;
use std::path::Path;
use std::sync::{atomic::AtomicI32, Arc};

pub struct RocksDB {
    base: Arc<BaseEngine>,
    pub db: DB,
    // rocksdb arc reference count for not release
    pub arc_count: AtomicI32,
    wo: WriteOptions,
}

impl Deref for RocksDB {
    type Target = Arc<BaseEngine>;
    fn deref<'a>(&'a self) -> &'a Arc<BaseEngine> {
        &self.base
    }
}

pub const ID_CF: &'static str = "id";

impl RocksDB {
    pub fn new(base: Arc<BaseEngine>) -> ASResult<RocksDB> {
        let db_path = base.base_path().join(Path::new("db"));
        let mut option = rocksdb::Options::default();
        option.create_if_missing(true);
        option.create_missing_column_families(true);
        let db = DB::open_cf(&option, db_path.to_str().unwrap(), &[ID_CF])?;

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        write_options.set_sync(false);

        Ok(RocksDB {
            base: base,
            db: db,
            arc_count: AtomicI32::new(1),
            wo: write_options,
        })
    }

    pub fn id_cf(&self) -> &ColumnFamily {
        self.db.cf_handle(ID_CF).unwrap()
    }

    pub fn get_doc_by_id<K: AsRef<[u8]> + std::fmt::Debug>(
        &self,
        key: K,
    ) -> ASResult<Option<Vec<u8>>> {
        match self.db.get_cf(self.id_cf(), key) {
            Ok(v) => Ok(v),
            Err(e) => result!(Code::DocumentNotFound, "get id key:{:?} has err", e),
        }
    }

    pub fn write(&self, key: &Vec<u8>, value: &Vec<u8>) -> ASResult<()> {
        let mut batch = WriteBatch::default();
        batch.put(key, value);
        conver(self.db.write_opt(batch, &self.wo))?;
        Ok(())
    }

    pub fn write_batch(&self, batch: WriteBatch) -> ASResult<()> {
        conver(self.db.write_opt(batch, &&self.wo))?;
        Ok(())
    }

    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> ASResult<()> {
        let mut batch = WriteBatch::default();
        batch.delete(key);
        conver(self.db.write_opt(batch, &&self.wo))?;
        Ok(())
    }

    pub fn estimate_count(&self) -> ASResult<u64> {
        Ok(self
            .db
            .property_int_value("rocksdb.estimate-num-keys")?
            .unwrap_or(0))
    }

    pub fn count(&self) -> ASResult<u64> {
        let prefix = [2];

        let iter = self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward));

        let mut count = 0;

        for (k, _) in iter {
            if k[0] >= 4 {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    pub fn write_raft_index(&self, raft_index: u64) -> ASResult<()> {
        let mut batch = WriteBatch::default();
        batch.put(RAFT_INDEX_KEY, &u64_slice(raft_index)[..]);
        conver(self.db.write_opt(batch, &&self.wo))?;
        Ok(())
    }

    pub fn read_raft_index(&self) -> ASResult<u64> {
        match self.db.get(RAFT_INDEX_KEY)? {
            Some(bs) => Ok(slice_u64(bs.as_slice())),
            None => Ok(0),
        }
    }

    pub fn find_max_iid(&self) -> u32 {
        let iter = self.db.iterator_cf(self.id_cf(), IteratorMode::End); // From a key in Direction::{forward,reverse}

        for (k, _) in iter {
            return slice_u32(&k);
        }

        return 0;
    }

    // query rocksdb range key ,  the range key > prefix , end  ,
    pub fn prefix_range(
        &self,
        prefix: Vec<u8>,
        mut f: impl FnMut(&[u8], &[u8]) -> ASResult<bool>,
    ) -> ASResult<()> {
        let iter = self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward)); // From a key in Direction::{forward,reverse}

        for (k, v) in iter {
            if !f(&*k, &*v)? {
                break;
            }
        }

        Ok(())
    }
}

impl Engine for RocksDB {
    fn flush(&self) -> ASResult<()> {
        let mut flush_options = FlushOptions::default();
        flush_options.set_wait(true);
        self.db.flush_cf(self.id_cf())?;
        conver(self.db.flush_opt(&flush_options))
    }

    fn release(&self) {
        info!(
            "the collection:{} , partition:{} to release",
            self.partition.collection_id, self.partition.id
        );
        let mut flush_options = FlushOptions::default();
        flush_options.set_wait(true);
        if let Err(e) = self.db.flush_opt(&flush_options) {
            error!("flush db has err:{:?}", e);
        }
    }
}

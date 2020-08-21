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
use crate::util::coding::*;
use crate::util::config::Config;
use crate::util::entity::{entity_key, MakeKey};
use crate::util::error::*;
use crate::util::time::*;
use crate::*;
use log::error;
use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions, DB};
use serde::{de::DeserializeOwned, Serialize};

use std::path::Path;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

pub struct RocksDB {
    db: Arc<DB>,
    id_lock: Mutex<usize>,
}

impl RocksDB {
    /// Init method
    pub fn new(conf: Arc<Config>) -> ASResult<RocksDB> {
        let path = Path::new(&conf.self_master().unwrap().data)
            .join(Path::new("meta"))
            .join(Path::new("db"));

        let path_dir = path.to_str().unwrap();
        let mut option = rocksdb::Options::default();
        option.set_wal_dir(path.join("wal").to_str().unwrap());
        option.create_if_missing(true);

        Ok(RocksDB {
            db: Arc::new(DB::open(&option, path_dir)?),
            id_lock: Mutex::new(1),
        })
    }

    //do put with bytes
    pub fn do_put(&self, key: &[u8], value: &[u8]) -> ASResult<()> {
        let mut batch = WriteBatch::default();
        batch.put(key, value);
        self.do_write_batch(batch)
    }

    pub fn do_put_batch(&self, kvs: &Vec<(Vec<u8>, Vec<u8>)>) -> ASResult<()> {
        let mut batch = WriteBatch::default();
        for kv in kvs {
            batch.put(&kv.0, &kv.1);
        }
        self.do_write_batch(batch)
    }

    //do create with bytes
    pub fn do_create(&self, key: &[u8], value: &[u8]) -> ASResult<()> {
        match self.do_get(key) {
            Ok(..) => {
                return result!(
                    Code::AlreadyExists,
                    "the key:{:?} already exists",
                    String::from_utf8(key.to_vec())
                )
            }
            Err(e) => {
                if e.code() != Code::RocksDBNotFound {
                    return Err(e);
                }
            }
        }

        let mut batch = WriteBatch::default();
        batch.put(key, value);
        self.do_write_batch(batch)
    }

    pub fn do_delete(&self, key: &[u8]) -> ASResult<()> {
        let mut batch = WriteBatch::default();
        batch.delete(key);
        self.do_write_batch(batch)
    }

    pub fn do_delete_batch(&self, keys: &Vec<Vec<u8>>) -> ASResult<()> {
        let mut batch = WriteBatch::default();
        for key in keys {
            batch.delete(key);
        }
        self.do_write_batch(batch)
    }

    /// do get
    pub fn do_get(&self, key: &[u8]) -> ASResult<Vec<u8>> {
        if key.len() == 0 {
            return result!(Code::ParamError, "key is empty");
        }

        match self.db.get(key) {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
                None => result!(Code::RocksDBNotFound, "key:[{:?}] not found!", key,),
            },
            Err(e) => result_def!("get key:{:?} has err:{}", key, e.to_string()),
        }
    }

    pub fn do_prefix_list(&self, prefix: &[u8]) -> ASResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = vec![];
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix, Direction::Forward)); // From a key in Direction::{forward,reverse}
        for (k, v) in iter {
            if !k.starts_with(prefix) {
                break;
            }
            result.push((k.to_vec(), v.to_vec()));
        }
        Ok(result)
    }

    /// do write
    fn do_write_batch(&self, batch: WriteBatch) -> ASResult<()> {
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(false);
        write_options.set_sync(true);
        conver(self.db.write_opt(batch, &write_options))
    }

    pub fn increase_id(&self, key: &[u8]) -> ASResult<u32> {
        let _lock = self.id_lock.lock().unwrap();
        let value = match self.do_get(key) {
            Err(e) => {
                if e.code() != Code::RocksDBNotFound {
                    return Err(e);
                }
                1
            }
            Ok(v) => slice_u32(&v.as_slice()) + 1,
        };

        self.do_put(key, &u32_slice(value)[..])?;
        Ok(value)
    }
}

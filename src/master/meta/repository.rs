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

pub struct HARepository {
    partition_lock: Mutex<u32>,
    lock: Mutex<u32>,
    write_lock: RwLock<u32>,
    db: Arc<DB>,
}

impl HARepository {
    /// Init method
    pub fn new(conf: Arc<Config>) -> ASResult<HARepository> {
        let path = Path::new(&conf.self_master().unwrap().data)
            .join(Path::new("meta"))
            .join(Path::new("db"));

        let path_dir = path.to_str().unwrap();
        let mut option = rocksdb::Options::default();
        option.set_wal_dir(path.join("wal").to_str().unwrap());
        option.create_if_missing(true);

        Ok(HARepository {
            partition_lock: Mutex::new(1),
            lock: Mutex::new(1),
            write_lock: RwLock::new(1),
            db: Arc::new(DB::open(&option, path_dir)?),
        })
    }

    //to add a lock by master key is key str, value is  u64(timeout_mill) + addr
    pub fn lock(&self, key: &str, ttl_mill: i64) -> ASResult<String> {
        let _lock = self.lock.lock().unwrap();

        let key = entity_key::lock(key);

        if let Some(value) = self.db.get(key.as_bytes())? {
            let time_out = slice_u64(&value) as i64;
            if current_millis() >= time_out {
                return result!(Code::ParamError, "has already lockd");
            }
        }

        let lease = uuid::Uuid::new_v4().to_string();

        let mut batch = WriteBatch::default();

        let mut value = Vec::new();
        value.extend((current_millis() + ttl_mill).to_be_bytes().to_vec());
        value.extend(lease.as_bytes());

        batch.put(key.as_bytes(), value.as_slice());
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(false);
        write_options.set_sync(true);
        conver(self.db.write_opt(batch, &write_options))?;

        Ok(lease)
    }

    //The contract lock
    pub fn lock_keep_alive(&self, key: &str, lease: &str, ttl_mill: i64) -> ASResult<()> {
        let _lock = self.lock.lock().unwrap();

        let key = entity_key::lock(key);

        match self.db.get(key.as_bytes())? {
            Some(v) => {
                let time_out = slice_u64(&v);
                if String::from_utf8_lossy(&v[8..]) != lease {
                    return result!(Code::LockedLeaseExpried, "lease not locked for key");
                }

                if current_millis() >= time_out as i64 {
                    return result!(Code::LockedLeaseExpried, "lease is expried");
                }
            }
            None => return result!(Code::LockedLeaseExpried, "not locked for key"),
        };

        let mut batch = WriteBatch::default();

        let mut value = Vec::new();
        value.extend((current_millis() + ttl_mill).to_be_bytes().to_vec());
        value.extend(lease.as_bytes());

        batch.put(key.as_bytes(), value.as_slice());
        self.do_write_batch(batch)
    }

    pub fn unlock(&self, key: &str, lease: &str) -> ASResult<()> {
        let _lock = self.lock.lock().unwrap();

        let key = entity_key::lock(key);

        match self.db.get(key.as_bytes())? {
            Some(v) => {
                if String::from_utf8_lossy(&v[8..]) != lease {
                    return result!(Code::LockedLeaseExpried, "lease not locked for key");
                }
            }
            None => return result!(Code::LockedLeaseExpried, "not locked for key"),
        };
        let mut batch = WriteBatch::default();
        batch.delete(key);
        self.do_write_batch(batch)
    }

    pub fn create<T: Serialize + MakeKey>(&self, value: &T) -> ASResult<()> {
        let key = value.make_key();
        let key = key.as_str();
        let _lock = self.write_lock.write().unwrap();
        match self.do_get(key) {
            Ok(_) => return result!(Code::AlreadyExists, "the key:{} already exists", key),
            Err(e) => {
                if e.code() != Code::RocksDBNotFound {
                    return Err(e);
                }
            }
        };
        self.do_put_json(key, value)
    }

    pub fn put<T: Serialize + MakeKey>(&self, value: &T) -> ASResult<()> {
        let key = value.make_key();
        let _lock = self.write_lock.read().unwrap();
        self.do_put_json(key.as_str(), value)
    }

    pub fn put_batch<T: Serialize + MakeKey>(&self, values: &Vec<T>) -> ASResult<()> {
        let _lock = self.write_lock.read().unwrap();
        let mut kvs: Vec<(String, &T)> = vec![];
        for value in values {
            kvs.push((value.make_key(), value));
        }
        self.do_put_jsons(kvs)
    }

    pub fn put_kv(&self, key: &str, value: &[u8]) -> ASResult<()> {
        let _lock = self.write_lock.read().unwrap();
        self.do_put(key, value)
    }

    pub fn get<T: DeserializeOwned>(&self, key: &str) -> ASResult<T> {
        let value = self.do_get(key)?;
        conver(serde_json::from_slice(value.as_slice()))
    }

    pub fn get_kv(&self, key: &str) -> ASResult<Vec<u8>> {
        self.do_get(key)
    }

    pub fn delete<T: Serialize + MakeKey>(&self, value: &T) -> ASResult<()> {
        let key = value.make_key();
        let _lock = self.write_lock.read().unwrap();
        let mut batch = WriteBatch::default();
        batch.delete(key.as_bytes());
        self.do_write_batch(batch)
    }

    pub fn delete_keys(&self, keys: Vec<String>) -> ASResult<()> {
        let _lock = self.write_lock.read().unwrap();
        let mut batch = WriteBatch::default();
        for key in keys {
            batch.delete(key.as_bytes());
        }
        self.do_write_batch(batch)
    }

    /// do put json
    fn do_put_json<T: Serialize>(&self, key: &str, value: &T) -> ASResult<()> {
        match serde_json::to_vec(value) {
            Ok(v) => self.do_put(key, v.as_slice()),
            Err(e) => result_def!("cast to json bytes err:{}", e.to_string()),
        }
    }

    /// do put json
    fn do_put_jsons<T: Serialize>(&self, kvs: Vec<(String, &T)>) -> ASResult<()> {
        let mut batch = WriteBatch::default();

        for kv in kvs {
            match serde_json::to_vec(kv.1) {
                Ok(v) => batch.put(kv.0.as_bytes(), v.as_slice()),
                Err(e) => return result_def!("cast to json bytes err:{}", e.to_string()),
            }
        }

        self.do_write_batch(batch)
    }

    //do put with bytes
    fn do_put(&self, key: &str, value: &[u8]) -> ASResult<()> {
        let mut batch = WriteBatch::default();
        batch.put(key.as_bytes(), value);
        self.do_write_batch(batch)
    }

    /// do get
    fn do_get(&self, key: &str) -> ASResult<Vec<u8>> {
        if key.len() == 0 {
            return result!(Code::ParamError, "key is empty");
        }

        match self.db.get(key.as_bytes()) {
            Ok(ov) => match ov {
                Some(v) => Ok(v),
                None => result!(Code::RocksDBNotFound, "key:[{}] not found!", key,),
            },
            Err(e) => result_def!("get key:{} has err:{}", key, e.to_string()),
        }
    }

    pub fn list<T: DeserializeOwned>(&self, prefix: &str) -> ASResult<Vec<T>> {
        let list = self.do_prefix_list(prefix)?;
        let mut result = Vec::with_capacity(list.len());
        for (_, v) in list {
            match serde_json::from_slice(v.as_slice()) {
                Ok(t) => result.push(t),
                Err(e) => {
                    error!("deserialize value to json has err:{:?}", e);
                }
            }
        }
        Ok(result)
    }

    fn do_prefix_list(&self, prefix: &str) -> ASResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = vec![];
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_bytes(), Direction::Forward)); // From a key in Direction::{forward,reverse}
        for (k, v) in iter {
            let k_str = String::from_utf8(k.to_vec()).unwrap();
            if !k_str.starts_with(prefix) {
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

    pub fn increase_id(&self, key: &str) -> ASResult<u32> {
        let _lock = self.partition_lock.lock().unwrap();

        let key = entity_key::lock(key);
        let key = key.as_str();

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

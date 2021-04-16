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
mod db;

use crate::util::coding::*;
use crate::util::config::Config;
use crate::util::entity::{entity_key, MakeKey};
use crate::util::error::*;
use crate::util::time::*;
use crate::*;
use tracing::log::error;
use serde::{de::DeserializeOwned, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

pub struct HARepository {
    partition_lock: Mutex<usize>,
    lock: Mutex<usize>,
    write_lock: RwLock<usize>,
}

impl HARepository {
    /// Init method
    pub async fn new(conf: Arc<Config>) -> ASResult<HARepository> {
        let db = db::RocksDB::new(conf.clone())?;
        Ok(HARepository {
            partition_lock: Mutex::new(1),
            lock: Mutex::new(1),
            write_lock: RwLock::new(1),
        })
    }

    pub async fn create<T: Serialize + MakeKey>(&self, value: &T) -> ASResult<()> {
        panic!()
        // let cmd = conver(serde_json::to_vec(&Command::Create {
        //     key: value.make_key().as_bytes().to_vec(),
        //     value: conver(serde_json::to_vec(value))?,
        // }))?;

        // self.raft.submit(cmd, true).await.map_err(|e| e.into())

    }

    pub async fn put<T: Serialize + MakeKey>(&self, value: &T) -> ASResult<()> {
        panic!()
        // let cmd = conver(serde_json::to_vec(&Command::Put {
        //     key: value.make_key().as_bytes().to_vec(),
        //     value: conver(serde_json::to_vec(value))?,
        // }))?;

        // self.raft.submit(cmd, true).await.map_err(|e| e.into())

    }

    pub async fn put_batch<T: Serialize + MakeKey>(&self, values: &Vec<T>) -> ASResult<()> {
        panic!()
        // let mut kvs: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        // for value in values {
        //     kvs.push((
        //         value.make_key().as_bytes().to_vec(),
        //         conver(serde_json::to_vec(value))?,
        //     ));
        // }
        // let cmd = conver(serde_json::to_vec(&Command::PutBatch { kvs }))?;
        // self.raft.submit(cmd, true).await.map_err(|e| e.into())

    }

    pub async fn put_kv(&self, key: &str, value: &[u8]) -> ASResult<()> {
        panic!()
        // let cmd = conver(serde_json::to_vec(&Command::Put {
        //     key: key.as_bytes().to_vec(),
        //     value: value.to_vec(),
        // }))?;
        // self.raft.submit(cmd, true).await.map_err(|e| e.into())

    }

    pub async fn delete<T: Serialize + MakeKey>(&self, value: &T) -> ASResult<()> {
        panic!()
        // let cmd = conver(serde_json::to_vec(&Command::Delete {
        //     key: value.make_key().as_bytes().to_vec(),
        // }))?;
        // self.raft.submit(cmd, true).await.map_err(|e| e.into())

    }

    pub async fn delete_keys(&self, keys: Vec<String>) -> ASResult<()> {
        panic!()
        // let keys = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
        // let cmd = conver(serde_json::to_vec(&Command::DeleteBatch { keys }))?;
        // self.raft.submit(cmd, true).await.map_err(|e| e.into())

    }
}

impl HARepository {
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> ASResult<T> {
        panic!()
        // let cmd = conver(serde_json::to_vec(&CommandForward::Get {
        //     key: key.as_bytes().to_vec(),
        // }))?;
        // let result = match self.raft.execute(cmd, true).await {
        //     Ok(r) => r,
        //     Err(e) => return Err(e.into()),
        // };
        // conver(serde_json::from_slice(&result))
    }

    pub async fn get_kv(&self, key: &str) -> ASResult<Vec<u8>> {
        panic!()
        // let cmd = conver(serde_json::to_vec(&CommandForward::Get {
        //     key: key.as_bytes().to_vec(),
        // }))?;
        // self.raft.execute(cmd, true).await.map_err(|e| e.into())

    }

    pub async fn list<T: DeserializeOwned>(&self, prefix: &str) -> ASResult<Vec<T>> {
        panic!()
        // let cmd = conver(serde_json::to_vec(&CommandForward::List {
        //     prefix: prefix.as_bytes().to_vec(),
        // }))?;

        // let value = match self.raft.execute(cmd, true).await {
        //     Ok(r) => r,
        //     Err(e) => return Err(e.into()),
        // };
        //
        // let list: Vec<(Vec<u8>, Vec<u8>)> = conver(serde_json::from_slice(&value))?;
        //
        // let mut result = Vec::new();
        // for v in list {
        //     result.push(conver(serde_json::from_slice(&v.1))?);
        // }
        // Ok(result)
    }

    pub async fn increase_id(&self, key: &str) -> ASResult<u32> {
        panic!()
        // let cmd = conver(serde_json::to_vec(&CommandForward::IncreaseId {
        //     key: key.as_bytes().to_vec(),
        // }))?;

        // let value = match self.raft.execute(cmd, true).await {
        //     Ok(r) => r,
        //     Err(e) => return Err(e.into()),
        // };
        //
        // Ok(util::coding::slice_u32(&value))
    }
}

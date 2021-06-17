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
use crate::meta::cmd::*;
use crate::meta::store::HARepository;
use crate::sleep;
use crate::util::time::*;
use crate::util::{coding, config::Config, entity::*, error::*};
use crate::*;
use alaya_protocol::pserver::*;
use rand::seq::SliceRandom;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::log::{debug, error, info, warn};

pub struct MasterService {
    pub meta_service: HARepository,
    partition_lock: RwLock<usize>,
    collection_lock: Mutex<usize>,
}

impl MasterService {
    pub async fn new(conf: Arc<Config>) -> ASResult<MasterService> {
        Ok(MasterService {
            meta_service: HARepository::new(conf).await?,
            partition_lock: RwLock::new(0),
            collection_lock: Mutex::new(0),
        })
    }

    pub async fn del_collection(&self, collection_name: &str) -> ASResult<Collection> {
        panic!();
    }

    pub async fn create_collection(&self, mut collection: Collection) -> ASResult<Collection> {
        panic!();
    }

    pub async fn get_collection(&self, collection_name: &str) -> ASResult<Collection> {
        panic!();
    }

    pub async fn get_collection_by_id(&self, collection_id: u32) -> ASResult<Collection> {
        panic!();
    }

    pub async fn list_collections(&self) -> ASResult<Vec<Collection>> {
        panic!();
    }

    pub async fn update_server(&self, mut server: PServer) -> ASResult<PServer> {
        panic!();
    }

    pub async fn list_servers(&self) -> ASResult<Vec<PServer>> {
        panic!();
    }

    pub async fn get_server(&self, server_addr: &str) -> ASResult<PServer> {
        panic!();
    }

    pub async fn register(&self, mut server: PServer) -> ASResult<PServer> {
        panic!();
    }

    pub async fn get_server_addr(&self, server_id: u32) -> ASResult<String> {
        panic!();
    }

    pub async fn list_partitions(&self, collection_name: &str) -> ASResult<Vec<Partition>> {
        panic!();
    }

    pub async fn list_partitions_by_id(&self, collection_id: u32) -> ASResult<Vec<Partition>> {
        panic!();
    }

    pub async fn get_partition(
        &self,
        collection_id: u32,
        partition_id: u32,
    ) -> ASResult<Partition> {
        panic!();
    }

    async fn load_or_create_partition(
        &self,
        addr: &str,
        collection_id: u32,
        partition_id: u32,
        term: u64,
    ) -> ASResult<GeneralResponse> {
        panic!();
    }

    async fn offload_partition(
        &self,
        collection_id: u32,
        partition_id: u32,
        term: u64,
    ) -> ASResult<()> {
        panic!();
    }
}

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
use crate::sleep;
use crate::util::time::*;
use crate::util::{
    coding,
    config::{Config, Master},
    entity::*,
    error::*,
    raft::{network::RaftNetwork, *},
};
use crate::*;
use alaya_protocol::pserver::*;
use alaya_protocol::raft::{write_action, Entry as RaftEntry, WriteActions};
use rand::seq::SliceRandom;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::log::{debug, error, info, warn};
use xraft::NodeId;
use xraft::Raft;

pub struct MasterService {
    raft: xraft::Raft<NodeId, WriteActions>,
    partition_lock: RwLock<usize>,
    collection_lock: Mutex<usize>,
}

impl MasterService {
    pub async fn new(conf: Arc<Config>) -> ASResult<MasterService> {
        // let members = conf
        //     .masters
        //     .iter()
        //     .map(|m| (m.node_id, Arc::new(m.clone())))
        //     .collect();

        let master = conf.self_master().unwrap();

        let mut option = rocksdb::Options::default();
        option.create_if_missing(true);
        let db = rocksdb::DB::open(&option, master.data.as_str())?;

        let raft = Raft::new(
            format!("master_{}", master.node_id),
            master.node_id,
            Arc::new(xraft::Config {
                heartbeat_interval: master.raft.heartbeat_interval,
                election_timeout_min: master.raft.election_timeout_min,
                election_timeout_max: master.raft.election_timeout_max,
                max_payload_entries: master.raft.max_payload_entries,
                to_voter_threshold: master.raft.to_voter_threshold,
            }),
            Arc::new(RaftStorage::new(Arc::new(db), None)),
            Arc::new(RaftNetwork::default()),
        )?;

        Ok(MasterService {
            raft,
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

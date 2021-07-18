// Copyright 2020 The Chubao Aut r#type: (), kv: ()  r#type: (), kv: ()  r#type: (), kv: () hors.
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
use crate::util::raft::network::Member;
use crate::util::time::*;
use crate::util::{
    coding,
    config::{Config, Meta},
    error::*,
    raft::{network::RaftNetwork, *},
};
use crate::*;
use alaya_protocol::pserver::*;
use alaya_protocol::raft::Kv;
use alaya_protocol::raft::WriteAction;
use alaya_protocol::raft::{write_action, Entry as RaftEntry, WriteActions};
use rand::seq::SliceRandom;
use std::fmt::format;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::log::{debug, error, info, warn};
use xraft::{NodeId, RaftError};
use xraft::Raft;
use crate::util::raft::storage::{RaftStorage, CF_LOG, CF_DATA};
use alaya_protocol::meta::{Collection, Partition, PServer};
use crate::meta::entity_key;
use crate::util::coding::u32_slice;
use prost::Message;

pub struct Service {
    storage: Arc<RaftStorage>,
    raft: Arc<xraft::Raft<NodeId, WriteActions>>,
    partition_lock: RwLock<usize>,
    collection_lock: Mutex<usize>,
}

impl Service {
    pub async fn new(conf: Arc<Config>, meta: &Meta) -> ASResult<Self> {
        let mut option = rocksdb::Options::default();
        option.create_if_missing(true);
        option.create_missing_column_families(true);
        option.set_allow_mmap_reads(true);
        let storage = Arc::new(RaftStorage::new(Arc::new(rocksdb::DB::open_cf(&option, meta.data.as_str(), &[CF_LOG, CF_DATA])?), None));

        let raft = Arc::new(Raft::new(
            format!("meta_{}", meta.node_id),
            meta.node_id,
            Arc::new(xraft::Config {
                heartbeat_interval: meta.raft.heartbeat_interval,
                election_timeout_min: meta.raft.election_timeout_min,
                election_timeout_max: meta.raft.election_timeout_max,
                max_payload_entries: meta.raft.max_payload_entries,
                to_voter_threshold: meta.raft.to_voter_threshold,
            }),
            storage.clone(),
            Arc::new(RaftNetwork::default()),
        )?);

        for m in &conf.metas {
            raft.add_non_voter(m.node_id, m.node_id).await.expect("raft add voter has err");
        }

        Ok(Self {
            storage,
            raft,
            partition_lock: RwLock::new(0),
            collection_lock: Mutex::new(0),
        })
    }

    pub async fn del_collection(&self, collection_name: &str) -> ASResult<Collection> {
        self.raft.client_read().await?;
        let collection = self.get_collection(collection_name)?;
        self.raft
            .client_write(WriteActions {
                actions: vec![WriteAction {
                    r#type: write_action::Type::Delete as i32,
                    kv: Some(Kv {
                        key: collection_name.to_owned().into_bytes(),
                        value: vec![],
                    }),
                }],
            })
            .await?;
        Ok(collection)
    }

    pub async fn create_collection(&self, mut collection: Collection) -> ASResult<Collection> {
        self.raft.client_read().await?;

        collection.id = self.sequence(entity_key::SEQ_COLLECTION.as_bytes()).await?;

        //do some thing
        let mut was = Vec::with_capacity(3);

        was.push(WriteAction {
            r#type: write_action::Type::Put as i32,
            kv: Some(Kv { key: entity_key::collection_name(&collection.name).into_bytes(), value: u32_slice(collection.id).to_vec() }),
        });


        was.push(WriteAction {
            r#type: write_action::Type::Put as i32,
            kv: Some(Kv { key: entity_key::collection(collection.id).into_bytes(), value: collection.encode_to_vec() }),
        });


        self.raft.client_write(WriteActions { actions: was }).await?;

        Ok(collection)
    }


    pub async fn update_server(&self, mut server: PServer) -> ASResult<PServer> {
        panic!();
    }


    pub async fn register(&self, mut server: PServer) -> ASResult<PServer> {
        panic!();
    }

    pub async fn list_partitions(&self, collection_name: &str) -> ASResult<Vec<Partition>> {
        panic!();
    }

    pub async fn list_partitions_by_id(&self, collection_id: u32) -> ASResult<Vec<Partition>> {
        panic!();
    }
}


//impl for storage
impl Service {
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

    pub fn get_collection(&self, name: &str) -> ASResult<Collection> {
        let value = self
            .storage
            .get_raw(entity_key::collection_name(name).as_str())?.ok_or(err!(Code::CollectionNotFound, "collection:[{}] not found!", name))?;

        self.get_collection_by_id(coding::slice_u32(&value))
    }

    pub fn get_collection_by_id(&self, id: u32) -> ASResult<Collection> {
        self.storage.get(entity_key::collection(id).as_str())?.ok_or(err!(Code::CollectionNotFound, "collection_id:[{}] not found!", id))
    }

    pub fn list_collections(&self) -> ASResult<Vec<Collection>> {
        Ok(self.storage.iterator(entity_key::PREFIX_COLLECTION.as_bytes(), None).map(|(_, v)| v).collect())
    }

    pub fn list_servers(&self) -> ASResult<Vec<PServer>> {
        Ok(self.storage.iterator(entity_key::PREFIX_PSERVER.as_bytes(), None).map(|(_, v)| v).collect())
    }

    pub fn get_server(&self, id: u32) -> ASResult<PServer> {
        self.storage.get(entity_key::pserver_id(id))?.ok_or(err!(Code::ServerNotFound, "server:[{}] not found!", id))
    }

    pub async fn sequence(&self, key: &[u8]) -> ASResult<u32> {
        let mut value = vec![];
        for x in 0..10 {
            let result = self.storage.get_raw(key)?.map_or_else(|| 1, |v| coding::slice_u32(&v));
            match self.raft.client_write(WriteActions {
                actions: vec![WriteAction {
                    r#type: write_action::Type::Sequence as i32,
                    kv: Some(Kv {
                        key: key.to_owned(),
                        value: coding::u32_slice(result + 1).into(),
                    }),
                }],
            })
                .await {
                Ok(()) => return Ok(result),
                Err(RaftError::Storage(e)) => {
                    if let Some(e) = (*e).downcast_ref::<ASError>() {
                        if e.code() == Code::SequenceNotExpect {
                            value.push(result);
                            continue;
                        }
                    }
                    return Err(ASError::from(RaftError::Storage(e)));
                }
                Err(e) => {
                    return Err(ASError::from(e));
                }
            }
        }

        result!(Code::SequenceRetryTooMay, "keys:{:?} try:{:?}", key, value)
    }
}

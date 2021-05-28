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
use crate::client::partition_client::PartitionClient;
use crate::client::ps_client::PsClient;
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
    ps_cli: PsClient,
    pub meta_service: HARepository,
    partition_lock: RwLock<usize>,
    collection_lock: Mutex<usize>,
}

impl MasterService {
    pub async fn new(conf: Arc<Config>) -> ASResult<MasterService> {
        Ok(MasterService {
            ps_cli: PsClient::new(conf.clone()),
            meta_service: HARepository::new(conf).await?,
            partition_lock: RwLock::new(0),
            collection_lock: Mutex::new(0),
        })
    }

    pub async fn del_collection(&self, collection_name: &str) -> ASResult<Collection> {
        let _lock = self.collection_lock.lock().await;
        //1.query collection
        let c: Collection = self.get_collection(collection_name).await?;

        //delete collection
        self.meta_service
            .delete_keys(vec![
                entity_key::collection_name(collection_name),
                entity_key::collection(c.id),
            ])
            .await?;

        //3.offload partition
        for pid in c.partitions.iter() {
            if let Err(e) = self.offload_partition(c.id, *pid, 0).await {
                error!(
                    "offload collection:{} partition:{} has err:{:?}",
                    c.id, pid, e
                );
            }
        }

        Ok(c)
    }

    pub async fn hibernate_collection(&self, collection_name: &str) -> ASResult<Collection> {
        let _lock = self.collection_lock.lock().await;
        //1.query collection
        let mut c: Collection = self.get_collection(collection_name).await?;

        match c.status {
            CollectionStatus::WORKING | CollectionStatus::HIBERNATE => {}
            _ => {
                return result!(
                    Code::ParamError,
                    "collection:[{}] status is:[{:#?}] can to hibernate",
                    collection_name,
                    c.status
                );
            }
        }

        c.status = CollectionStatus::HIBERNATE;
        self.meta_service.put(&c).await?;

        //3.offload partition
        for pid in c.partitions.iter() {
            if let Err(e) = self.offload_partition(c.id, *pid, 0).await {
                error!(
                    "offload collection:{} partition:{} has err:{:?}",
                    c.id, pid, e
                );
            }
        }

        Ok(c)
    }

    pub async fn create_collection(&self, mut collection: Collection) -> ASResult<Collection> {
        info!("begin to create collection");
        let _lock = self.collection_lock.lock().await;

        collection.validate()?;

        //check collection exists
        match self.get_collection(&collection.name).await {
            Ok(_) => {
                return result!(
                    Code::AlreadyExists,
                    "collection:{} already exists",
                    collection.name
                )
            }
            Err(e) => {
                if e.code() != Code::CollectionNotFound {
                    return Err(e);
                }
            }
        }

        let seq = self
            .meta_service
            .increase_id(entity_key::SEQ_COLLECTION)
            .await?;

        info!("no coresponding collection found, begin to create connection ");
        info!("all fields valid.");
        collection.id = seq;
        collection.status = CollectionStatus::CREATING;
        collection.modify_time = current_millis();

        let partition_num = collection.partition_num;

        if partition_num == 0 {
            return result_def!("partition_num:{} is invalid", partition_num);
        }

        let partition_replica_num = collection.partition_replica_num;

        if partition_replica_num == 0 {
            return result_def!("partition_replica_num:{} is invalid", partition_replica_num);
        }

        let server_list: Vec<PServer> = self
            .meta_service
            .list(entity_key::pserver_prefix().as_str())
            .await?;

        if partition_replica_num as usize > server_list.len() {
            return result_def!(
                "need pserver size:{} but all server is:{}",
                partition_replica_num,
                server_list.len()
            );
        }

        let mut use_list: Vec<PServer> = Vec::new();
        for s in server_list {
            match self.ps_cli.status(s.get_addr()).await {
                Ok(gr) => match Code::from_i32(gr.code) {
                    Code::EngineWillClose => warn!("ps:{} will close so skip", s.get_addr()),
                    _ => use_list.push(s),
                },
                Err(e) => {
                    error!("conn ps:{} has err:{:?}", s.addr.as_str(), e);
                }
            };
        }

        if partition_replica_num as usize > use_list.len() {
            return result_def!(
                "need pserver size:{} but available server is:{}",
                partition_replica_num,
                use_list.len()
            );
        }

        let mut all_list = Vec::new();
        for _ in 0..partition_num {
            use_list.choose(&mut rand::thread_rng());
            for i in 0..partition_replica_num {
                all_list.push(use_list[i as usize].clone());
            }
        }
        drop(use_list);

        let mut partitions = Vec::with_capacity(partition_num as usize);
        let mut pids = Vec::with_capacity(partition_num as usize);
        let mut slots = Vec::with_capacity(partition_num as usize);
        let range = u32::max_value() / partition_num;
        for i in 0..partition_num {
            let server = all_list.get(i as usize).unwrap();
            pids.push(i);
            slots.push(i * range);
            let mut replicas: Vec<Replica> = Vec::new();
            for j in 0..partition_replica_num {
                let id = all_list
                    .get((i * partition_replica_num + j) as usize)
                    .unwrap()
                    .id
                    .unwrap();
                replicas.push(Replica {
                    node_id: id,
                    replica_type: ReplicaType::NORMAL,
                });
            }
            let partition = Partition {
                id: i,
                collection_id: seq,
                leader: server.addr.to_string(),
                term: AtomicU64::new(0),
                replicas: replicas,
            };

            partitions.push(partition.clone());
        }

        collection.slots = slots;
        collection.partitions = pids;

        info!("prepare add collection partitions len:{}", partitions.len());

        self.meta_service.create(&collection).await?;
        self.meta_service.put_batch(&partitions).await?;
        for c in partitions {
            let mut replicas: Vec<ReplicaInfo> = vec![];
            for r in c.replicas {
                replicas.push(ReplicaInfo {
                    node: r.node_id,
                    replica_type: r.replica_type as u32,
                });
            }
            PartitionClient::new(c.leader)
                .load_or_create_partition(PartitionRequest {
                    partition_id: c.id,
                    collection_id: c.collection_id,
                    readonly: false,
                    term: 0,
                    replicas: replicas,
                })
                .await?;
        }

        let start = crate::util::time::current_millis();
        for index in 0..partition_num {
            loop {
                if let Ok(partition) = self.get_partition(collection.id, index).await {
                    if partition.load_term() > 0 {
                        break;
                    } else {
                        if crate::util::time::current_millis() - start > 120000 {
                            return result!(Code::Timeout, "crate collection timeout 2m");
                        }
                    }
                } else {
                    return result!(
                        Code::InternalErr,
                        "collection:{} partition:{} not found, it may be a bug",
                        collection.id,
                        index
                    );
                }
                info!("wait collection:{} partition:{}", collection.id, index);
                crate::sleep!(200);
            }
            info!("collection:{} partition:{} init ok", collection.name, index);
        }
        println!("========================================");
        collection.status = CollectionStatus::WORKING;
        self.meta_service.put(&collection).await?;
        println!("========================================123123123xxxxxxx");
        self.meta_service
            .put_kv(
                entity_key::collection_name(collection.name.as_str()).as_str(),
                &coding::u32_slice(collection.id)[..],
            )
            .await?;
        println!("========================================123123123");
        Ok(collection)
    }

    pub async fn get_collection(&self, collection_name: &str) -> ASResult<Collection> {
        let value = self
            .meta_service
            .get_kv(entity_key::collection_name(collection_name).as_str())
            .await
            .map_err(|e| {
                if e.code() == Code::RocksDBNotFound {
                    err!(
                        Code::CollectionNotFound,
                        "partition name:{} not found",
                        collection_name
                    )
                } else {
                    e
                }
            })?;

        self.get_collection_by_id(coding::slice_u32(&value[..]))
            .await
    }

    pub async fn get_collection_by_id(&self, collection_id: u32) -> ASResult<Collection> {
        self.meta_service
            .get(entity_key::collection(collection_id).as_str())
            .await
            .map_err(|e| {
                if e.code() == Code::RocksDBNotFound {
                    err!(
                        Code::CollectionNotFound,
                        "partition id:{} not found",
                        collection_id
                    )
                } else {
                    e
                }
            })
    }

    pub async fn list_collections(&self) -> ASResult<Vec<Collection>> {
        self.meta_service
            .list(entity_key::collection_prefix().as_str())
            .await
    }

    pub async fn update_server(&self, mut server: PServer) -> ASResult<PServer> {
        server.modify_time = current_millis();
        self.meta_service.put(&server).await?;
        return Ok(server);
    }

    pub async fn list_servers(&self) -> ASResult<Vec<PServer>> {
        self.meta_service
            .list(entity_key::pserver_prefix().as_str())
            .await
    }

    pub async fn get_server(&self, server_addr: &str) -> ASResult<PServer> {
        self.meta_service
            .get(entity_key::pserver(server_addr).as_str())
            .await
            .map_err(|e| {
                if e.code() == Code::RocksDBNotFound {
                    err!(
                        Code::PServerNotFound,
                        "paserver addr:{} not found",
                        server_addr
                    )
                } else {
                    e
                }
            })
    }

    pub async fn register(&self, mut server: PServer) -> ASResult<PServer> {
        match self.get_server(server.addr.clone().as_ref()).await {
            Ok(ps) => Ok(ps),
            Err(e) => {
                if e.code() != Code::PServerNotFound {
                    return Err(e);
                }
                let seq = self
                    .meta_service
                    .increase_id(entity_key::SEQ_PSERVER)
                    .await?;
                server.id = Some(seq);

                match self
                    .meta_service
                    .put_kv(
                        &entity_key::pserver_id(seq).as_str(),
                        &server.addr.as_bytes(),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(e) => return Err(e),
                }
                match self.meta_service.create(&server).await {
                    Ok(_) => {
                        return Ok(server);
                    }
                    Err(e) => {
                        if e.code() != Code::AlreadyExists {
                            return Err(e);
                        }
                        match self.get_server(&server.addr.as_str()).await {
                            Ok(pserver) => {
                                return Ok(pserver);
                            }
                            Err(e) => Err(e),
                        }
                    }
                }
            }
        }
    }

    pub async fn get_server_addr(&self, server_id: u32) -> ASResult<String> {
        match self
            .meta_service
            .get_kv(entity_key::pserver_id(server_id).as_str())
            .await
        {
            Ok(v) => match String::from_utf8(v) {
                Ok(v) => Ok(v),
                Err(e) => result_def!("Invalid server addr UTF-8 sequence:{:?}", e),
            },
            Err(e) => Err(e),
        }
    }

    pub async fn list_partitions(&self, collection_name: &str) -> ASResult<Vec<Partition>> {
        let value = self
            .meta_service
            .get_kv(entity_key::collection_name(collection_name).as_str())
            .await
            .map_err(|e| {
                if e.code() == Code::RocksDBNotFound {
                    err!(
                        Code::CollectionNotFound,
                        "partition name:{} not found",
                        collection_name
                    )
                } else {
                    e
                }
            })?;

        self.list_partitions_by_id(coding::slice_u32(&value[..]))
            .await
    }

    pub async fn list_partitions_by_id(&self, collection_id: u32) -> ASResult<Vec<Partition>> {
        self.meta_service
            .list(entity_key::partition_prefix(collection_id).as_str())
            .await
    }

    pub async fn get_partition(
        &self,
        collection_id: u32,
        partition_id: u32,
    ) -> ASResult<Partition> {
        self.meta_service
            .get(entity_key::partiition(collection_id, partition_id).as_str())
            .await
    }

    pub async fn transfer_partition(&self, mut ptransfer: PTransfer) -> ASResult<()> {
        let (cid, pid, to_server) = (
            ptransfer.collection_id,
            ptransfer.partition_id,
            ptransfer.to_server.as_str(),
        );
        info!(
            "try to offload partition with [collection_id:{}, partition_id:{},to_server: {}]",
            cid, pid, to_server
        );

        self.ps_cli.status(to_server).await?; //validate can be transfer

        let old_partition = self.get_partition(cid, pid).await?;
        let (old_term, old_addr) = (old_partition.load_term(), old_partition.leader);

        for i in 0..100 as u8 {
            info!("try to transfer partition times:{}", i);

            if i > 90 {
                warn!("to retry long times so make it back:{}", old_addr);
                ptransfer.to_server = old_addr.clone();
            }

            if let Err(e) = self.offload_partition(cid, pid, old_term).await {
                if e.code() == Code::VersionErr {
                    return Err(e);
                }
                sleep!(300);
                continue;
            } else {
                info!("offload collection:{} partition:{} success.", cid, pid);
            }

            sleep!(300);

            match self
                .load_or_create_partition(
                    ptransfer.to_server.as_str(),
                    ptransfer.collection_id,
                    ptransfer.partition_id,
                    old_term,
                )
                .await
            {
                Ok(_) => {
                    info!("load collection:{} partition:{} success.", cid, pid);
                    return Ok(());
                }
                Err(e) => {
                    if e.code() == Code::VersionErr {
                        return Err(e);
                    }
                    sleep!(300);
                    continue;
                }
            }
        }
        return result_def!("tansfer has err");
    }

    async fn load_or_create_partition(
        &self,
        addr: &str,
        collection_id: u32,
        partition_id: u32,
        term: u64,
    ) -> ASResult<GeneralResponse> {
        info!(
            "try to create or load collection:{} partition:{}",
            collection_id, partition_id
        );

        let partition = self.get_partition(collection_id, partition_id).await?;

        //check version
        if partition.load_term() > term {
            return result!(
                Code::VersionErr,
                "load version has version err expected:{} , found:{} ",
                term,
                partition.load_term(),
            );
        }

        // load begin to try offload partition, try not to repeat the load
        for ps in self.list_servers().await? {
            for wp in ps.write_partitions {
                if (wp.collection_id, wp.id) == (collection_id, partition_id) {
                    return result!(
                        Code::PartitionLoadErr,
                        "partition has been used in server:{}",
                        ps.addr
                    );
                }
            }
        }

        PartitionClient::new(addr.to_string())
            .load_or_create_partition(PartitionRequest {
                collection_id: collection_id,
                partition_id: partition_id,
                readonly: false,
                term: term,
                replicas: vec![],
            })
            .await
    }

    async fn offload_partition(
        &self,
        collection_id: u32,
        partition_id: u32,
        term: u64,
    ) -> ASResult<()> {
        for ps in self.list_servers().await? {
            for wp in ps.write_partitions {
                if (wp.collection_id, wp.id) == (collection_id, partition_id) {
                    PartitionClient::new(ps.addr.clone())
                        .offload_partition(PartitionRequest {
                            collection_id: collection_id,
                            partition_id: partition_id,
                            readonly: false,
                            term: term,
                            replicas: vec![],
                        })
                        .await?;
                }
            }
        }

        let par = self.get_partition(collection_id, partition_id).await?;

        PartitionClient::new(par.leader.clone())
            .offload_partition(PartitionRequest {
                collection_id: collection_id,
                partition_id: partition_id,
                readonly: false,
                term: term,
                replicas: vec![],
            })
            .await?;

        Ok(())
    }

    pub async fn update_partition(&self, partition: Partition) -> ASResult<()> {
        let _lock = self.partition_lock.write().await;
        match self
            .get_partition(partition.collection_id, partition.id)
            .await
        {
            Ok(p) => {
                if p.load_term() > partition.load_term() {
                    return result!(
                        Code::VersionErr,
                        "the collection:{} partition:{} version not right expected:{} found:{}",
                        partition.collection_id,
                        partition.id,
                        partition.load_term(),
                        p.load_term()
                    );
                }
            }
            Err(e) => {
                if e.code() != Code::RocksDBNotFound {
                    return Err(e);
                }
            }
        }
        self.meta_service.put(&partition).await
    }
}

#[test]
fn test_json_schema() {
    let collection_schema = "{\"name\": \"t1\",\"partition_num\": 1,\"replica_num\": 1,\"fields\": [{\"name\": \"name\", \"type\": \"string\", \"index\": true, \"store\": true, \"array\": false }, { \"name\": \"age\", \"type\": \"int\", \"index\": true, \"store\": true, \"array\": false } ]}";
    let collection_value: serde_json::value::Value = serde_json::from_str(collection_schema)
        .expect(format!("collection to json has err:{}", collection_schema).as_str());
    match collection_value.get("name") {
        Some(s) => info!("{}", s.as_str().unwrap()),
        None => panic!("not found"),
    }
}

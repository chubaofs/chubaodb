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
use crate::master::cmd::*;
use crate::master::meta::repository::HARepository;
use crate::pserverpb::*;
use crate::sleep;
use crate::util::time::*;
use crate::util::{coding, config::Config, entity::*, error::*};
use log::{error, info, warn};
use serde_json::json;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

pub struct MasterService {
    ps_cli: PsClient,
    pub meta_service: HARepository,
    partition_lock: RwLock<usize>,
    collection_lock: Mutex<usize>,
}

impl MasterService {
    pub fn new(conf: Arc<Config>) -> ASResult<MasterService> {
        Ok(MasterService {
            ps_cli: PsClient::new(conf.clone()),
            meta_service: HARepository::new(conf)?,
            partition_lock: RwLock::new(0),
            collection_lock: Mutex::new(0),
        })
    }

    pub fn start(&self) -> ASResult<()> {
        match self.meta_service.put(&Zone {
            id: Some(0),
            name: Some(String::from("default")),
        }) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn create_zone(&self, zone: Zone) -> ASResult<Zone> {
        self.meta_service.create(&zone)?;
        Ok(zone)
    }

    pub async fn del_collection(&self, collection_name: &str) -> ASResult<Collection> {
        let _lock = self.collection_lock.lock().unwrap();
        //1.query collection
        let c: Collection = self.get_collection(collection_name)?;

        let cid = c.id.unwrap();
        //delete collection
        self.meta_service.delete_keys(vec![
            entity_key::collection_name(collection_name),
            entity_key::collection(c.id.unwrap()),
        ])?;

        //3.offload partition
        for pid in c.partitions.as_ref().unwrap() {
            if let Err(e) = self.offload_partition(cid, *pid, 0).await {
                error!(
                    "offload collection:{} partition:{} has err:{:?}",
                    cid, pid, e
                );
            }
        }

        Ok(c)
    }

    pub async fn create_collection(&self, mut collection: Collection) -> ASResult<Collection> {
        info!("begin to create collection");
        let _lock = self.collection_lock.lock().unwrap();

        //check collection exists
        match self.get_collection(collection.get_name()) {
            Ok(_) => {
                return Err(err_code_box(
                    ALREADY_EXISTS,
                    format!("the collection:{} already exist", collection.get_name()),
                ))
            }
            Err(e) => {
                let e = cast_to_err(e);
                if e.0 != NOT_FOUND {
                    return Err(e);
                }
            }
        }

        let seq = self.meta_service.increase_id(entity_key::SEQ_COLLECTION)?;

        info!("no coresponding collection found, begin to create connection ");
        let mut vector_index = Vec::new();
        if collection.fields.len() > 0 {
            for (i, f) in collection.get_mut_fields().iter_mut().enumerate() {
                validate_and_set_field(f)?;
                if f.internal_type == FieldType::VECTOR {
                    vector_index.push(i);
                }
            }
        }
        info!("all fields valid.");
        collection.id = Some(seq);
        collection.status = Some(CollectionStatus::CREATING);
        collection.modify_time = Some(current_millis());
        collection.vector_field_index = vector_index;

        //let zones = &collection.zones;
        let need_num = collection.partition_num.unwrap();

        // TODO default zone ID
        let server_list: Vec<PServer> = self
            .meta_service
            .list(entity_key::pserver_prefix().as_str())?;

        if need_num as usize > server_list.len() {
            return Err(Box::from(err(format!(
                "need pserver size:{} but all server is:{}",
                need_num,
                server_list.len()
            ))));
        }
        let mut use_list: Vec<PServer> = Vec::new();
        //from list_server find need_num for use
        for s in server_list.iter() {
            let ok = match self.ps_cli.status(s.addr.as_str()).await {
                Ok(gr) => match gr.code as u16 {
                    ENGINE_WILL_CLOSE => false,
                    _ => true,
                },
                Err(e) => {
                    error!("conn ps:{} has err:{:?}", s.addr.as_str(), e);
                    false
                }
            };
            if !ok {
                continue;
            }
            use_list.push(s.clone());
            if use_list.len() >= need_num as usize {
                break;
            }
        }

        let mut partitions = Vec::with_capacity(need_num as usize);
        let mut pids = Vec::with_capacity(need_num as usize);
        let mut slots = Vec::with_capacity(need_num as usize);
        let range = u32::max_value() / need_num;

        for i in 0..need_num {
            let server = use_list.get(i as usize).unwrap();
            pids.push(i);
            slots.push(i * range);
            let partition = Partition {
                id: i,
                collection_id: seq,
                leader: server.addr.to_string(),
                version: 0,
            };
            partitions.push(partition.clone());
        }

        collection.slots = Some(slots);
        collection.partitions = Some(pids);

        let collection_name = collection.name.clone().unwrap();
        info!("prepare add collection info:{}", partitions.len());

        self.meta_service.create(&collection)?;

        for c in partitions {
            PartitionClient::new(c.leader)
                .load_or_create_partition(PartitionRequest {
                    partition_id: c.id,
                    collection_id: c.collection_id,
                    readonly: false,
                    version: 0,
                })
                .await?;
        }

        collection.status = Some(CollectionStatus::WORKING);
        self.meta_service.put(&collection)?;
        self.meta_service.put_kv(
            entity_key::collection_name(collection_name.as_str()).as_str(),
            &coding::u32_slice(collection.id.unwrap())[..],
        )?;
        Ok(collection)
    }

    pub fn get_collection(&self, collection_name: &str) -> ASResult<Collection> {
        let value = self
            .meta_service
            .get_kv(entity_key::collection_name(collection_name).as_str())?;

        self.get_collection_by_id(coding::slice_u32(&value[..]))
    }

    pub fn get_collection_by_id(&self, collection_id: u32) -> ASResult<Collection> {
        self.meta_service
            .get(entity_key::collection(collection_id).as_str())
    }

    pub fn list_collections(&self) -> ASResult<Vec<Collection>> {
        self.meta_service
            .list(entity_key::collection_prefix().as_str())
    }

    pub fn update_server(&self, mut server: PServer) -> ASResult<PServer> {
        server.modify_time = current_millis();
        self.meta_service.put(&server)?;
        Ok(server)
    }

    pub fn list_servers(&self) -> ASResult<Vec<PServer>> {
        self.meta_service
            .list(entity_key::pserver_prefix().as_str())
    }

    pub fn get_server(&self, server_addr: &str) -> ASResult<PServer> {
        self.meta_service
            .get(entity_key::pserver(server_addr).as_str())
    }

    pub fn list_zones(&self) -> ASResult<Vec<Zone>> {
        self.meta_service.list(entity_key::zone_prefix().as_str())
    }

    pub fn list_partitions(&self, collection_name: &str) -> ASResult<Vec<Partition>> {
        let value = self
            .meta_service
            .get_kv(entity_key::collection_name(collection_name).as_str())?;

        self.list_partitions_by_id(coding::slice_u32(&value[..]))
    }

    pub fn list_partitions_by_id(&self, collection_id: u32) -> ASResult<Vec<Partition>> {
        self.meta_service
            .list(entity_key::partition_prefix(collection_id).as_str())
    }

    pub fn get_partition(&self, collection_id: u32, partition_id: u32) -> ASResult<Partition> {
        self.meta_service
            .get(entity_key::partiition(collection_id, partition_id).as_str())
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

        let old_partition = self.get_partition(cid, pid)?;
        let (old_addr, old_version) = (old_partition.leader, old_partition.version);

        for i in 0..100 as u8 {
            info!("try to transfer partition times:{}", i);

            if i > 90 {
                warn!("to retry long times so make it back:{}", old_addr);
                ptransfer.to_server = old_addr.clone();
            }

            if let Err(e) = self.offload_partition(cid, pid, old_version).await {
                error!(
                    "offload collection:{} partition:{} failed. err:{:?}",
                    cid, pid, e
                );
                let e = cast_to_err(e);
                if e.0 == VERSION_ERR {
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
                    old_version,
                )
                .await
            {
                Ok(_) => {
                    info!("load collection:{} partition:{} success.", cid, pid);
                    return Ok(());
                }
                Err(e) => {
                    error!(
                        "load collection:{} partition:{} failed. err:{:?}",
                        cid, pid, e
                    );
                    let e = cast_to_err(e);
                    if e.0 == VERSION_ERR {
                        return Err(e);
                    }
                    sleep!(300);
                    continue;
                }
            }
        }
        return Err(err_str_box("tansfer has err"));
    }

    async fn load_or_create_partition(
        &self,
        addr: &str,
        collection_id: u32,
        partition_id: u32,
        version: u64,
    ) -> ASResult<GeneralResponse> {
        info!(
            "try to create or load collection:{} partition:{}",
            collection_id, partition_id
        );

        let partition = self.get_partition(collection_id, partition_id)?;

        //check version
        if partition.version > version {
            return Err(err_code_box(
                VERSION_ERR,
                format!(
                    "load version has version err expected:{} , found:{} ",
                    version, partition.version,
                ),
            ));
        }

        // load begin to try offload partition, try not to repeat the load
        for ps in self.list_servers()? {
            for wp in ps.write_partitions {
                if (wp.collection_id, wp.id) == (collection_id, partition_id) {
                    return Err(err_code_box(
                        PARTITION_CAN_NOT_LOAD,
                        format!("partition has been used in server:{}", ps.addr),
                    ));
                }
            }
        }

        PartitionClient::new(addr.to_string())
            .load_or_create_partition(PartitionRequest {
                collection_id: collection_id,
                partition_id: partition_id,
                readonly: false,
                version: version,
            })
            .await
    }

    async fn offload_partition(
        &self,
        collection_id: u32,
        partition_id: u32,
        version: u64,
    ) -> ASResult<()> {
        for ps in self.list_servers()? {
            for wp in ps.write_partitions {
                if (wp.collection_id, wp.id) == (collection_id, partition_id) {
                    PartitionClient::new(ps.addr.clone())
                        .offload_partition(PartitionRequest {
                            collection_id: collection_id,
                            partition_id: partition_id,
                            readonly: false,
                            version: version,
                        })
                        .await?;
                }
            }
        }

        let par = self.get_partition(collection_id, partition_id)?;

        PartitionClient::new(par.leader.clone())
            .offload_partition(PartitionRequest {
                collection_id: collection_id,
                partition_id: partition_id,
                readonly: false,
                version: version,
            })
            .await?;

        Ok(())
    }

    pub async fn update_partition(&self, partition: Partition) -> ASResult<()> {
        let _lock = self.partition_lock.write().unwrap();
        match self.get_partition(partition.collection_id, partition.id) {
            Ok(p) => {
                if p.version >= partition.version {
                    return Err(err_code_box(
                        VERSION_ERR,
                        format!(
                            "the collection:{} partition:{} version not right expected:{} found:{}",
                            partition.collection_id, partition.id, partition.version, p.version
                        ),
                    ));
                }
            }
            Err(e) => {
                let e = cast_to_err(e);
                if e.0 != NOT_FOUND {
                    return Err(e);
                }
            }
        }
        self.meta_service.put(&partition)
    }
}

fn validate_and_set_field(field: &mut Field) -> ASResult<()> {
    if field.name.is_none() {
        return Err(err_box(format!("unset field name in field:{:?}", field)));
    }

    match field.field_type.as_ref() {
        Some(v) => match v.as_str() {
            "text" => field.internal_type = FieldType::TEXT,
            "string" => field.internal_type = FieldType::STRING,
            "integer" => field.internal_type = FieldType::INTEGER,
            "double" => field.internal_type = FieldType::DOUBLE,
            "vector" => {
                //here is validate type is ok , and must param and some default param
                field.internal_type = FieldType::VECTOR;
                let _: i32 = field.get_option_value(field_option::DIMENSION)?;

                let map = field.option.as_object_mut().unwrap();

                if map.get(field_option::TRAIN_SIZE).is_none() {
                    map.insert(field_option::TRAIN_SIZE.to_string(), json!(20000));
                }
            }
            _ => {
                return Err(err_box(format!(
                    "unknow field:{} type:{}",
                    field.name.as_ref().unwrap(),
                    v
                )));
            }
        },
        None => {
            return Err(err_box(format!(
                "the field:{} must set type",
                field.name.as_ref().unwrap()
            )));
        }
    };

    Ok(())
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

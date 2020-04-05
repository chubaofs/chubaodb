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
use crate::pserverpb::*;
use crate::util::error::*;
use crate::util::time::*;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum FieldType {
    UNKNOW = 0,
    STRING = 1,
    INTEGER = 2,
    DOUBLE = 3,
    TEXT = 4,
    VECTOR = 5,
}

//about field option
pub mod field_option {
    pub const DIMENSION: &'static str = "dimension"; //i32
    pub const TRAIN_SIZE: &'static str = "train_size"; //usize
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Field {
    pub name: Option<String>,
    pub field_type: Option<String>,
    #[serde(default)]
    pub array: bool,
    #[serde(default = "def_json_value")]
    pub option: Value,
    #[serde(default = "def_field_type")]
    pub internal_type: FieldType,
}

impl Field {
    pub fn get_option_value<T: DeserializeOwned>(&self, name: &str) -> ASResult<T> {
        if let Some(v) = self.option.get(name) {
            convert(serde_json::from_value(v.clone()))
        } else {
            Err(err_code_box(
                NOT_FOUND,
                format!(
                    "not found field:{} in {}'s option",
                    name,
                    self.name.as_ref().unwrap()
                ),
            ))
        }
    }
}

fn def_json_value() -> Value {
    json!({})
}

fn def_field_type() -> FieldType {
    FieldType::UNKNOW
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum CollectionStatus {
    UNKNOW = 0,
    CREATING = 1,
    DROPED = 2,
    WORKING = 3,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Collection {
    pub id: Option<u32>,
    pub name: Option<String>,
    #[serde(default)]
    pub fields: Vec<Field>,
    pub partition_num: Option<u32>,
    pub partition_replica_num: Option<u32>,
    pub partitions: Option<Vec<u32>>,
    pub slots: Option<Vec<u32>>,
    pub status: Option<CollectionStatus>,
    pub modify_time: Option<u64>,
    #[serde(default)] //target has vector field
    pub vector_field_index: Vec<usize>,
}

impl Collection {
    pub fn get_name(&self) -> &str {
        self.name.as_ref().unwrap().as_str()
    }

    pub fn get_mut_fields(&mut self) -> &mut Vec<Field> {
        self.fields.as_mut()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Partition {
    pub id: u32,
    pub collection_id: u32,
    pub leader: String,
    pub version: u64,
    pub replicas: Vec<Replica>,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Replica {
    pub node: u32,
    pub peer: u64,
    pub replica_type: ReplicaType,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ReplicaType {
    NORMAL = 0,  //normal type
    LEARNER = 1, //learner type
}

impl Partition {
    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn get_collection_id(&self) -> u32 {
        self.collection_id
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PServer {
    pub id: Option<u32>,
    pub addr: String,
    #[serde(default)]
    pub write_partitions: Vec<Partition>,
    pub zone_id: u32,
    #[serde(default = "current_millis")]
    pub modify_time: u64,
}

impl PServer {
    pub fn new(zone_id: u32, id: Option<u32>, addr: String) -> Self {
        PServer {
            id: id,
            zone_id: zone_id,
            write_partitions: Vec::default(),
            addr: addr,
            modify_time: 0,
        }
    }

    pub fn get_addr(&self) -> &str {
        self.addr.as_str()
    }

    pub fn get_zone_id(&self) -> u32 {
        self.zone_id
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Zone {
    pub id: Option<u32>,
    pub name: Option<String>,
}

impl Zone {
    pub fn get_id(&self) -> u32 {
        self.id.unwrap()
    }

    pub fn get_name(&self) -> &str {
        self.name.as_ref().unwrap().as_str()
    }
}

pub fn merge_count_document_response(
    mut dist: CountDocumentResponse,
    src: CountDocumentResponse,
) -> CountDocumentResponse {
    if src.code != SUCCESS as i32 {
        dist.code = src.code;
    }
    dist.estimate_count += src.estimate_count;
    dist.index_count += src.index_count;
    if src.message.len() > 0 {
        dist.message.push_str("\n");
        dist.message.push_str(src.message.as_str());
    }

    dist
}

pub fn merge_search_document_response(
    mut dist: SearchDocumentResponse,
    mut src: SearchDocumentResponse,
) -> SearchDocumentResponse {
    if src.code != SUCCESS as i32 {
        dist.code = src.code;
    }

    dist.total = src.total + dist.total;
    dist.hits.append(&mut src.hits);

    dist.info = {
        let mut d = dist.info.unwrap_or(SearchInfo {
            success: 1,
            error: 0,
            message: String::default(),
        });
        match src.info {
            Some(s) => {
                d.success += s.success;
                d.error += s.error;
                if !s.message.is_empty() {
                    d.message.push_str("\n");
                    d.message.push_str(s.message.as_str());
                }
            }
            None => {
                d.success += 1;
            }
        }
        Some(d)
    };

    dist
}

pub trait MakeKey {
    fn make_key(&self) -> String;
}

/// META_ZONES_{zone_id} = value: {Zone}
impl MakeKey for Zone {
    fn make_key(&self) -> String {
        entity_key::zone(self.get_id())
    }
}

/// META_PARTITIONS_{collection_id}_{partition_id} = value: {Partition}
impl MakeKey for Partition {
    fn make_key(&self) -> String {
        entity_key::partiition(self.collection_id, self.id)
    }
}

/// META_COLLECTIONS_{collection_id}
impl MakeKey for Collection {
    fn make_key(&self) -> String {
        entity_key::collection(self.id.unwrap())
    }
}

/// META_SERVERS_{server_addr} = value: {PServer}
impl MakeKey for PServer {
    fn make_key(&self) -> String {
        entity_key::pserver(self.addr.as_str())
    }
}

pub mod entity_key {

    const PREFIX_ZONE: &str = "/META/ZONE";
    const PREFIX_PSERVER: &str = "/META/SERVER";
    const PREFIX_COLLECTION: &str = "/META/COLLECTION";
    const PREFIX_PARTITION: &str = "/META/PARTITION";
    const PREFIX_PSERVER_ID: &str = "/META/SERVER_ID";

    pub const SEQ_COLLECTION: &str = "/META/SEQUENCE/COLLECTION";
    pub const SEQ_PARTITION: &str = "/META/SEQUENCE/PARTITION";
    pub const SEQ_PSERVER: &str = "/META/SEQUENCE/PSERVER";

    pub fn zone(id: u32) -> String {
        format!("{}/{}", PREFIX_ZONE, id)
    }

    pub fn zone_prefix() -> String {
        format!("{}/", PREFIX_ZONE)
    }

    pub fn pserver(addr: &str) -> String {
        format!("{}/{}", PREFIX_PSERVER, addr)
    }
    pub fn pserver_prefix() -> String {
        format!("{}/", PREFIX_PSERVER)
    }

    pub fn pserver_id(server_id: u32) -> String {
        format!("{}/{}", PREFIX_PSERVER_ID, server_id)
    }

    pub fn collection(id: u32) -> String {
        format!("{}/{}", PREFIX_COLLECTION, id)
    }

    pub fn collection_prefix() -> String {
        format!("{}/", PREFIX_COLLECTION)
    }

    pub fn partiition(collection_id: u32, partiition_id: u32) -> String {
        format!("{}/{}/{}", PREFIX_PARTITION, collection_id, partiition_id)
    }

    pub fn partition_prefix(collection_id: u32) -> String {
        format!("{}/{}/", PREFIX_PARTITION, collection_id)
    }

    /// META_MAPPING_COLLECTION_{collection_name}
    pub fn collection_name(collection_name: &str) -> String {
        format!("META/MAPPING/COLLECTION/{}", collection_name)
    }

    /// META_LOCK_/{collection_name}
    pub fn lock(key: &str) -> String {
        format!("META/LOCK/{}", key)
    }
}

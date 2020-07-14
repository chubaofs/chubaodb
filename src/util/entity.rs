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
use crate::*;
use async_graphql::{Enum, InputObject};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub const ID_BYTES: &'static str = "_iid_bytes";

#[InputObject]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IntField {
    pub name: String,
    #[field(desc = "is array type of values", default = false)]
    #[serde(default = "default_false")]
    pub array: bool,
    #[field(desc = "value can miss", default = false)]
    #[serde(default = "default_false")]
    pub none: bool,
    #[field(
        desc = "is value to store it in column , if it need sort or get or aggregation",
        default = false
    )]
    #[serde(default = "default_false")]
    pub value: bool,
}

#[InputObject]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FloatField {
    pub name: String,
    #[field(desc = "is array type of values", default = false)]
    #[serde(default = "default_false")]
    pub array: bool,
    #[field(desc = "value can miss", default = false)]
    #[serde(default = "default_false")]
    pub none: bool,
    #[field(
        desc = "is value to store it in column , if it need sort or get or aggregation",
        default = false
    )]
    #[serde(default = "default_false")]
    pub value: bool,
}

fn default_false() -> bool {
    false
}

#[InputObject]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StringField {
    pub name: String,
    #[field(desc = "is array type of values", default = false)]
    #[serde(default = "default_false")]
    pub array: bool,
    #[field(desc = "value can miss", default = false)]
    #[serde(default = "default_false")]
    pub none: bool,
    #[field(
        desc = "is value to store it in column , if it need sort or get or aggregation",
        default = false
    )]
    #[serde(default = "default_false")]
    pub value: bool,
}

#[InputObject]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TextField {
    pub name: String,
    #[field(desc = "is array type of values", default = false)]
    #[serde(default = "default_false")]
    pub array: bool,
    #[field(desc = "value can miss", default = false)]
    #[serde(default = "default_false")]
    pub none: bool,
    #[field(
        desc = "is value to store it in column , if it need sort or get or aggregation",
        default = false
    )]
    #[serde(default = "default_false")]
    pub value: bool,
}

#[InputObject]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BytesField {
    pub name: String,
    #[field(desc = "value can miss", default = false)]
    #[serde(default = "default_false")]
    pub none: bool,
}

#[InputObject]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DateField {
    pub name: String,
    #[field(desc = "time str format", default = "auto")]
    pub format: String,
    #[field(desc = "is array type of values", default = false)]
    #[serde(default = "default_false")]
    pub array: bool,
    #[field(desc = "value can miss", default = false)]
    #[serde(default = "default_false")]
    pub none: bool,
    #[field(
        desc = "is value to store it in column , if it need sort or get or aggregation",
        default = false
    )]
    #[serde(default = "default_false")]
    pub value: bool,
}

#[Enum(desc = "computer method default is L2")]
#[derive(Serialize, Deserialize, Debug)]
pub enum MetricType {
    L2 = 1,
    InnerProduct = 2,
}

#[InputObject]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VectorField {
    pub name: String,
    #[field(desc = "is array type of values", default = false)]
    #[serde(default = "default_false")]
    pub array: bool,
    #[field(desc = "value can miss", default = false)]
    #[serde(default = "default_false")]
    pub none: bool,
    //train when doc got the size, if size <=0 , not train
    pub train_size: i32,
    //dimension: dimension of the input vectors
    pub dimension: i32,
    // A constructor
    pub description: String,
    // the type of metric
    pub metric_type: MetricType,
}

impl VectorField {
    pub fn validate(&self, v: Option<Value>) -> ASResult<Vec<f32>> {
        let none = v.is_none();
        if none && self.none {
            return Ok(Vec::default());
        }

        let value: Vec<f32> = serde_json::from_value(v.unwrap())?;

        if value.len() == 0 && none {
            return Ok(value);
        }

        if !self.array {
            if value.len() != self.dimension as usize {
                return result_def!(
                    "the field:{} vector dimension expectd:{} , found:{}",
                    self.name,
                    self.dimension,
                    value.len()
                );
            }
        } else {
            if value.len() % self.dimension as usize != 0 {
                return result_def!(
                    "the field:{} vector dimension expectd:{} * n  , found:{}  mod:{}",
                    self.name,
                    self.dimension,
                    value.len(),
                    value.len() % self.dimension as usize
                );
            }
        }

        return Ok(value);
    }
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Field {
    int(IntField),
    float(FloatField),
    string(StringField),
    text(TextField),
    bytes(BytesField),
    date(DateField),
    vector(VectorField),
}

impl Field {
    pub fn is_vector(&self) -> bool {
        matches!(*self, Field::vector(_))
    }

    pub fn vector(&self) -> ASResult<VectorField> {
        match self {
            Field::vector(f) => Ok(f.clone()),
            _ => result!(
                Code::FieldTypeErr,
                "schma field:{:?} type is not vecotr",
                self,
            ),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Field::int(f) => f.name.as_str(),
            Field::float(f) => f.name.as_str(),
            Field::string(f) => f.name.as_str(),
            Field::text(f) => f.name.as_str(),
            Field::bytes(f) => f.name.as_str(),
            Field::date(f) => f.name.as_str(),
            Field::vector(f) => f.name.as_str(),
        }
    }

    pub fn array(&self) -> bool {
        match self {
            Field::int(f) => f.array,
            Field::float(f) => f.array,
            Field::string(f) => f.array,
            Field::text(f) => f.array,
            Field::bytes(_) => false,
            Field::date(f) => f.array,
            Field::vector(f) => f.array,
        }
    }

    pub fn none(&self) -> bool {
        match self {
            Field::int(f) => f.none,
            Field::float(f) => f.none,
            Field::string(f) => f.none,
            Field::text(f) => f.none,
            Field::bytes(f) => f.none,
            Field::date(f) => f.none,
            Field::vector(f) => f.none,
        }
    }

    pub fn value(&self) -> bool {
        match self {
            Field::int(f) => f.value,
            Field::float(f) => f.value,
            Field::string(f) => f.value,
            Field::text(f) => f.value,
            Field::bytes(_) => true,
            Field::date(f) => f.value,
            _ => false,
        }
    }

    pub fn validate(&self, v: Option<&Value>) -> ASResult<()> {
        if v.is_none() {
            if self.none() {
                return Ok(());
            } else {
                return result!(Code::ParamError, "field:{} can not none", self.name(),);
            }
        }

        let v = v.unwrap();

        if self.array() {
            if !v.is_array() {
                return result!(
                    Code::ParamError,
                    "field:{} expect array but found:{:?} ",
                    self.name(),
                    v,
                );
            } else {
                for sv in v.as_array().unwrap() {
                    self.validate_field(sv)?;
                }
            }
        } else {
            self.validate_field(v)?;
        }
        Ok(())
    }

    fn validate_field(&self, v: &Value) -> ASResult<()> {
        match self {
            Field::int(f) => {
                if !v.is_i64() {
                    return result!(
                        Code::FieldTypeErr,
                        "field:{} expect int but found:{:?} ",
                        f.name,
                        v,
                    );
                }
            }
            Field::float(f) => {
                if !v.is_f64() {
                    return result!(
                        Code::FieldTypeErr,
                        "field:{} expect float but found:{:?} ",
                        f.name,
                        v,
                    );
                }
            }
            Field::string(f) => {
                if !v.is_string() {
                    return result!(
                        Code::FieldTypeErr,
                        "field:{} expect string but found:{:?} ",
                        f.name,
                        v,
                    );
                }
            }
            Field::text(f) => {
                if !v.is_string() {
                    return result!(
                        Code::FieldTypeErr,
                        "field:{} expect text but found:{:?} ",
                        f.name,
                        v,
                    );
                }
            }
            Field::bytes(f) => {
                if !v.is_string() {
                    return result!(
                        Code::FieldTypeErr,
                        "field:{} expect text but found:{:?} ",
                        f.name,
                        v,
                    );
                }
            }
            Field::date(f) => {
                if !v.is_number() && !v.is_string() {
                    return result!(
                        Code::FieldTypeErr,
                        "field:{} expect date but found:{:?} ",
                        f.name,
                        v,
                    );
                }
            }
            Field::vector(_) => {
                panic!("not vector field");
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum CollectionStatus {
    UNKNOW = 0,
    CREATING = 1,
    DROPED = 2,
    WORKING = 3,
}

impl Default for CollectionStatus {
    fn default() -> Self {
        CollectionStatus::UNKNOW
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(default)]
pub struct Collection {
    pub id: u32,
    pub name: String,
    pub fields: Vec<Field>,
    pub partition_num: u32,
    pub partition_replica_num: u32,
    pub partitions: Vec<u32>,
    pub slots: Vec<u32>,
    pub status: CollectionStatus,
    pub modify_time: u64,
    pub vector_field_index: Vec<usize>,
    pub scalar_field_index: Vec<usize>,
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
    pub node_id: u32,
    pub replica_type: ReplicaType,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ReplicaType {
    NORMAL = 0,
    //normal type
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
    #[serde(default)]
    pub zone: String,
    #[serde(default = "current_millis")]
    pub modify_time: u64,
}

impl PServer {
    pub fn new(zone: String, id: Option<u32>, addr: String) -> Self {
        PServer {
            id: id,
            zone: zone,
            write_partitions: Vec::default(),
            addr: addr,
            modify_time: 0,
        }
    }

    pub fn get_addr(&self) -> &str {
        self.addr.as_str()
    }
}

pub fn merge_count_document_response(
    mut dist: CountDocumentResponse,
    src: CountDocumentResponse,
) -> CountDocumentResponse {
    if Code::from_i32(src.code) != Code::Success {
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

pub fn msg_for_resp(info: &Option<SearchInfo>) -> String {
    match info {
        Some(i) => i.message.clone(),
        None => format!("not found info"),
    }
}

pub fn merge_search_document_response(
    mut dist: SearchDocumentResponse,
    mut src: SearchDocumentResponse,
) -> SearchDocumentResponse {
    if src.code != Code::Success as i32 {
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

pub fn merge_aggregation_response(
    mut dist: AggregationResponse,
    result: &mut HashMap<String, AggValues>,
    src: AggregationResponse,
) -> AggregationResponse {
    if src.code != Code::Success as i32 {
        dist.code = src.code;
    }

    dist.total = src.total + dist.total;

    merge_aggregation_result(result, src.result);

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

fn merge_aggregation_result(dist: &mut HashMap<String, AggValues>, src: Vec<AggValues>) {
    for v in src.into_iter() {
        if let Some(dv) = dist.get_mut(&v.key) {
            merge_aggregation_values(dv, v);
        } else {
            dist.insert(v.key.clone(), v);
        }
    }
}
fn merge_aggregation_values(dist: &mut AggValues, src: AggValues) {
    for (i, v) in src.values.into_iter().enumerate() {
        merge_aggregation_value(dist.values.get_mut(i).unwrap(), v);
    }
}

fn merge_aggregation_value(dist: &mut AggValue, src: AggValue) {
    match &mut dist.agg_value {
        Some(agg_value::AggValue::Stats(s)) => {
            if let Some(agg_value::AggValue::Stats(src)) = src.agg_value {
                s.count += src.count;
                s.max = src.max;
                s.min = src.min;
                s.sum = src.sum;
                s.missing = src.missing;
            } else {
                panic!("impossible agg result has none");
            }
        }
        Some(agg_value::AggValue::Hits(h)) => {
            if let Some(agg_value::AggValue::Hits(src)) = src.agg_value {
                h.count += src.count;

                if h.hits.len() as u64 >= h.size {
                    return;
                }

                for hit in src.hits {
                    h.hits.push(hit);
                    if h.hits.len() as u64 >= h.size {
                        return;
                    }
                }
            } else {
                panic!("impossible agg result has none");
            }
        }
        _ => panic!("impossible agg result has none"),
    }
    panic!()
}

pub trait MakeKey {
    fn make_key(&self) -> String;
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
        entity_key::collection(self.id)
    }
}

/// META_SERVERS_{server_addr} = value: {PServer}
impl MakeKey for PServer {
    fn make_key(&self) -> String {
        entity_key::pserver(self.addr.as_str())
    }
}

pub mod entity_key {
    const PREFIX_PSERVER: &str = "/META/SERVER";
    const PREFIX_COLLECTION: &str = "/META/COLLECTION";
    const PREFIX_PARTITION: &str = "/META/PARTITION";
    const PREFIX_PSERVER_ID: &str = "/META/SERVER_ID";

    pub const SEQ_COLLECTION: &str = "/META/SEQUENCE/COLLECTION";
    pub const SEQ_PARTITION: &str = "/META/SEQUENCE/PARTITION";
    pub const SEQ_PSERVER: &str = "/META/SEQUENCE/PSERVER";

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

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
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PTransfer {
    pub collection_id: u32,
    pub partition_id: u32,
    pub to_server: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PCreate {
    pub collection_name: String,
    pub partition_num: u32,
    pub zones: Vec<u32>,
}

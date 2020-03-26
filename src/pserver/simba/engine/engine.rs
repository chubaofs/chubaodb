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
use crate::util::{config, entity::*, error::ASResult};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub trait Engine {
    fn flush(&self) -> ASResult<()>;
    fn release(&self);
}

pub struct BaseEngine {
    pub conf: Arc<config::Config>,
    pub collection: Arc<Collection>,
    pub partition: Arc<Partition>,
}

impl BaseEngine {
    pub fn new(base: &BaseEngine) -> BaseEngine {
        BaseEngine {
            conf: base.conf.clone(),
            collection: base.collection.clone(),
            partition: base.partition.clone(),
        }
    }

    pub fn base_path(&self) -> PathBuf {
        Path::new(&self.conf.ps.data)
            .join(Path::new(
                format!("{}", self.partition.collection_id).as_str(),
            ))
            .join(Path::new(format!("{}", self.partition.id).as_str()))
    }
}

// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.

use crate::util::{config, entity::*};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub trait Engine {
    fn flush(&self, pre_db_sn: u64) -> Option<u64>;
    fn release(&self);
}

pub struct BaseEngine {
    pub conf: Arc<config::Config>,
    pub collection: Arc<Collection>,
    pub partition: Arc<Partition>,
    pub max_sn: RwLock<u64>,
}

impl BaseEngine {
    pub fn new(base: &BaseEngine) -> BaseEngine {
        BaseEngine {
            conf: base.conf.clone(),
            collection: base.collection.clone(),
            partition: base.partition.clone(),
            max_sn: RwLock::new(0),
        }
    }

    pub fn get_sn(&self) -> u64 {
        *self.max_sn.read().unwrap()
    }

    pub fn set_sn_if_max(&self, sn: u64) {
        let mut v = self.max_sn.write().unwrap();
        if *v < sn {
            *v = sn;
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

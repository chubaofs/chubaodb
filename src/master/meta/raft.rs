use crate::master::meta::rocksdb::RocksDB;
use crate::util::error::ASError;
use crate::util::{coding::*, config, entity::*};
use async_std::task;
use log::error;
use raft4rs::{entity::Config, error::*, state_machine::*};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct MasterStateMachine {
    db: RocksDB,
}

impl MasterStateMachine {
    pub fn new(db: RocksDB) -> Self {
        Self { db }
    }
}

pub enum Command {
    //write ,
    Create {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    putBatch {
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    },
    Delete {
        key: Vec<u8>,
    },
    DeleteBatch {
        keys: Vec<Vec<u8>>,
    },
    IncreaseId {
        key: Vec<u8>,
    },
    //read
    Get {
        key: Vec<u8>,
    },
    List {
        prefix: Vec<u8>,
    },
}

impl StateMachine for MasterStateMachine {
    fn apply_log(&self, _term: u64, index: u64, command: &[u8]) -> RaftResult<()> {
        let cmd: Command = serde_json::from_slice(command);
        // match cmd {
        //     Create { key, value } => self.db.put(key, value),
        // }

        panic!();
    }

    fn apply_member_change(
        &self,
        _term: u64,
        _index: u64,
        _node_id: u64,
        _action: u8,
        _exists: bool,
    ) -> RaftResult<()> {
        panic!()
    }

    fn apply_leader_change(&self, term: u64, _index: u64, leader: u64) -> RaftResult<()> {
        Ok(())
    }
}

pub fn make_raft_conf(conf: &Arc<config::Config>) -> Config {
    let master = conf.self_master().unwrap();
    let r = &master.raft;
    Config {
        node_id: r.node_id,
        heartbeat_port: r.heartbeat_port,
        replicate_port: r.replicate_port,
        log_path: std::path::Path::new(master.data.as_str())
            .join("raft")
            .to_str()
            .unwrap()
            .to_string(),
        log_max_num: r.log_max_num,
        log_min_num: r.log_min_num,
        log_file_size_mb: r.log_file_size_mb,
        heartbeate_ms: r.heartbeate_ms,
    }
}

pub fn make_resolver(conf: &Arc<config::Config>) -> MasterResolver {
    let resolver = MasterResolver::new();
    for m in conf.masters.iter() {
        let node_id = m.raft.node_id;
        resolver
            .log_addrs
            .insert(node_id, format!(m.ip, m.raft.log_port));
    }

    resolver
}

pub struct MasterResolver {
    log_addrs: HashMap<u64, String>,
    internal_addrs: HashMap<u64, String>,
    proxy_addrs: HashMap<u64, String>,
}

impl MasterResolver {
    pub fn new() -> MasterResolver {
        MasterResolver {
            log_addrs: HashMap::new(),
            internal_addrs: HashMap::new(),
            proxy_addrs: HashMap::new(),
        }
    }
}

impl Resolver for MasterResolver {
    fn heartbeat_addr(&self, node_id: &u64) -> RaftResult<String> {
        match self.internal_addrs.get(node_id) {
            Some(v) => Ok(v.to_string()),
            None => Err(RaftError::NotfoundAddr(*node_id)),
        }
    }

    fn log_addr(&self, node_id: &u64) -> RaftResult<String> {
        match self.log_addrs.get(node_id) {
            Some(v) => Ok(v.to_string()),
            None => Err(RaftError::NotfoundAddr(*node_id)),
        }
    }

    fn proxy_addr(&self, node_id: &u64) -> RaftResult<String> {
        match self.proxy_addrs.get(node_id) {
            Some(v) => Ok(v.to_string()),
            None => Err(RaftError::NotfoundAddr(*node_id)),
        }
    }
}

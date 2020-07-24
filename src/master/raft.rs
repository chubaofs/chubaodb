use crate::master::meta::repository::HARepository;
use crate::util::error::ASError;
use crate::util::{coding::*, config, entity::*};
use async_std::task;
use log::error;
use raft4rs::{entity::Config, error::*, state_machine::*};
use std::sync::{Arc, Mutex};

pub struct MasterStateMachine {
    meta_service: HARepository,
}

impl MasterStateMachine {
    pub fn new(meta_service: HARepository) -> Self {
        Self { meta_service }
    }
}

impl StateMachine for MasterStateMachine {
    fn apply_log(&self, _term: u64, index: u64, command: &[u8]) -> RaftResult<()> {
        Ok(())
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

pub fn make_raft_conf(node_id: u64, conf: &Arc<config::Config>) -> Config {
    let master = conf.self_master().unwrap();
    let r = &master.raft;
    Config {
        node_id: node_id,
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

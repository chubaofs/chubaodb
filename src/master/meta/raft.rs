use crate::master::meta::db::RocksDB;
use crate::util::error::ASError;
use crate::util::{coding::*, config::Config, entity::*};
use async_std::task;
use log::error;
use raft4rs::{
    entity::Config as RaftConfig, error::*, raft::Raft, server::Server, state_machine::*,
};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Command {
    //write ,
    Create { key: Vec<u8>, value: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
    PutBatch { kvs: Vec<(Vec<u8>, Vec<u8>)> },
    Delete { key: Vec<u8> },
    DeleteBatch { keys: Vec<Vec<u8>> },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum CommandForward {
    IncreaseId { key: Vec<u8> },
    Get { key: Vec<u8> },
    List { prefix: Vec<u8> },
}

pub async fn new(conf: &Arc<Config>, db: RocksDB) -> Arc<Raft> {
    let (rs, replicas) = make_resolver(conf);
    let server = Arc::new(Server::new(make_raft_conf(conf), rs)).start();

    server
        .create_raft(
            conf.self_master().unwrap().node_id,
            0,
            0,
            &replicas,
            MasterStateMachine { db: db },
        )
        .await
        .unwrap()
}

pub struct MasterStateMachine {
    db: RocksDB,
}

impl MasterStateMachine {
    pub fn new(db: RocksDB) -> Self {
        Self { db }
    }
}

impl StateMachine for MasterStateMachine {
    fn apply_log(&self, _term: u64, _index: u64, command: &[u8]) -> RaftResult<()> {
        let cmd: Command =
            serde_json::from_slice(command).map_err(|e| RaftError::Error(e.to_string()))?;

        match cmd {
            Command::Create { key, value } => self.db.do_create(&key, &value).map_err(|e| e.into()),
            Command::Put { key, value } => self.db.do_put(&key, &value).map_err(|e| e.into()),
            Command::PutBatch { kvs } => self.db.do_put_batch(&kvs).map_err(|e| e.into()),
            Command::Delete { key } => self.db.do_delete(&key).map_err(|e| e.into()),
            Command::DeleteBatch { keys } => self.db.do_delete_batch(&keys).map_err(|e| e.into()),
            _ => Err(RaftError::Error(format!(
                "apply_log not support this command:{:?}",
                cmd
            ))),
        }
    }

    fn execute(&self, command: &[u8]) -> RaftResult<Vec<u8>> {
        let cmd: CommandForward =
            serde_json::from_slice(command).map_err(|e| RaftError::Error(e.to_string()))?;
        match cmd {
            CommandForward::IncreaseId { key } => self
                .db
                .increase_id(&key)
                .map(|v| u32_slice(v).to_vec())
                .map_err(|e| e.into()),
            CommandForward::Get { key } => self.db.do_get(&key).map_err(|e| e.into()),
            CommandForward::List { prefix } => {
                let result = self.db.do_prefix_list(&prefix).map_err(|e| e.into())?;
                Ok(serde_json::to_vec(&result).unwrap())
            }
        }
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

pub fn make_raft_conf(conf: &Arc<Config>) -> RaftConfig {
    let master = conf.self_master().unwrap();
    let r = &master.raft;
    RaftConfig {
        node_id: master.node_id,
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

pub fn make_resolver(conf: &Arc<Config>) -> (MasterResolver, Vec<u64>) {
    let mut resolver = MasterResolver::new();
    let mut replicas = Vec::new();
    for m in conf.masters.iter() {
        let node_id = m.node_id;
        replicas.push(node_id);
        resolver
            .log_addrs
            .insert(node_id, format!("{}:{}", m.ip, m.raft.replicate_port));
    }

    (resolver, replicas)
}

pub struct MasterResolver {
    log_addrs: HashMap<u64, String>,
    internal_addrs: HashMap<u64, String>,
}

impl MasterResolver {
    pub fn new() -> MasterResolver {
        MasterResolver {
            log_addrs: HashMap::new(),
            internal_addrs: HashMap::new(),
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
}

use crate::client::meta_client::MetaClient;
use crate::pserver::service::PartitionService;
use crate::pserver::simba::simba::Simba;
use crate::util::error::ASError;
use crate::util::{coding::*, config, entity::*, error::Code::InternalErr};
use async_std::task;
use async_trait::async_trait;
use log::error;
use raft4rs::{entity::Config, error::*, state_machine::*};
use std::sync::{Arc, Mutex};

pub struct NodeStateMachine {
    simba: Option<Arc<Simba>>,
    collection: Arc<Collection>,
    partition: Arc<Partition>,
    ps: Arc<PartitionService>,
    term_lock: Mutex<usize>,
}

impl NodeStateMachine {
    pub fn new(
        simba: Option<Arc<Simba>>,
        collection: Arc<Collection>,
        partition: Arc<Partition>,
        ps: Arc<PartitionService>,
    ) -> NodeStateMachine {
        if simba.is_some() {
            simba.as_ref().unwrap().arc_count();
        };
        NodeStateMachine {
            simba,
            collection,
            partition,
            ps,
            term_lock: Mutex::new(0),
        }
    }
}

#[async_trait]
impl StateMachine for NodeStateMachine {
    fn apply_log(&self, _term: u64, index: u64, command: &[u8]) -> RaftResult<()> {
        if let Some(simba) = &self.simba {
            match simba.do_write(index, command, false) {
                Err(ASError::Error(c, m)) => Err(RaftError::ErrCode(c as i32, m)),
                _ => Ok(()),
            }
        } else {
            Ok(())
        }
    }

    async fn apply_member_change(
        &self,
        _term: u64,
        _index: u64,
        _node_id: u64,
        _action: u8,
        _exists: bool,
    ) -> RaftResult<()> {
        panic!()
    }

    async fn apply_leader_change(&self, term: u64, _index: u64, leader: u64) -> RaftResult<()> {
        {
            let _lock = self.term_lock.lock().unwrap();
            if self.partition.load_term() > term {
                return Err(RaftError::ErrCode(
                    InternalErr.into(),
                    format!(
                        "apply leader change has err:[term:{} less:{}]",
                        term,
                        self.partition.load_term()
                    ),
                ));
            }
            self.partition.set_term(term);
        }

        if let Err(e) = self
            .ps
            .apply_leader_change(&self.collection, &self.partition, leader)
            .await
        {
            error!("apply leader change has err:{}", e);
            return Err(RaftError::ErrCode(e.code() as i32, e.message()));
        };
        Ok(())
    }

    fn execute(&self, command: &[u8]) -> RaftResult<Vec<u8>> {
        panic!("imple me")
    }
}

pub struct NodeResolver {
    meta_client: Arc<MetaClient>,
}

impl NodeResolver {
    pub fn new(meta_client: Arc<MetaClient>) -> Self {
        Self {
            meta_client: meta_client,
        }
    }
}

impl Resolver for NodeResolver {
    fn heartbeat_addr(&self, node_id: &u64) -> RaftResult<String> {
        let pserver: PServer = task::block_on(self.meta_client.pserver_get(*node_id))
            .map_err(|e| RaftError::Error(e.to_string()))?;

        Ok(pserver.raft_heart_addr)
    }

    fn log_addr(&self, node_id: &u64) -> RaftResult<String> {
        let pserver: PServer = task::block_on(self.meta_client.pserver_get(*node_id))
            .map_err(|e| RaftError::Error(e.to_string()))?;

        Ok(pserver.raft_log_addr)
    }
}

pub fn make_raft_conf(node_id: u64, conf: &Arc<config::Config>) -> Config {
    let r = &conf.ps.raft;
    Config {
        node_id: node_id,
        heartbeat_port: r.heartbeat_port,
        replicate_port: r.replicate_port,
        log_path: std::path::Path::new(&conf.ps.data)
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

//event for data coding encoding
#[derive(PartialEq, Copy, Clone)]
pub enum EventType {
    Delete = 0,
    Create = 1,
    Update = 2,
}

pub enum Event {
    //key+ old_id+ 0
    Delete(Vec<u8>, Vec<u8>),
    //value+ key + len(key)+1
    Create(Vec<u8>, Vec<u8>),
    //value + key + len(k) + iid + 2
    Update(Vec<u8>, Vec<u8>, Vec<u8>),
}

impl Event {
    pub fn encode(self: Event) -> Vec<u8> {
        match self {
            Event::Delete(iid, mut k) => {
                let need_len = k.len() + iid.len() + 1;
                if k.capacity() < need_len {
                    k.reserve(need_len - k.capacity());
                }
                k.extend_from_slice(&iid);
                k.push(EventType::Delete as u8);
                k
            }
            Event::Create(k, mut v) => {
                let need_len = v.len() + k.len() + 3;
                if v.capacity() < need_len {
                    v.reserve(need_len - v.capacity());
                }
                v.extend_from_slice(&k);
                v.extend_from_slice(&u16_slice(k.len() as u16)[..]);
                v.push(EventType::Create as u8);
                v
            }
            Event::Update(iid, k, mut v) => {
                let need_len = v.len() + k.len() + 7;
                if v.capacity() < need_len {
                    v.reserve(need_len - v.capacity());
                }
                v.extend_from_slice(&k);
                v.extend_from_slice(&u16_slice(k.len() as u16)[..]);
                v.extend_from_slice(&iid);
                v.push(EventType::Update as u8);
                v
            }
        }
    }

    //iid  key value
    pub fn decode<'a>(data: &'a [u8]) -> (EventType, u32, &'a [u8], &'a [u8]) {
        let empty: &'static [u8] = &[0];
        //key+ old_id+ 0
        //value+ key + len(key)+1
        //value + key + len(k) + iid + 2
        let len = data.len() - 1;
        match data[len] {
            0 => (
                EventType::Delete,
                slice_u32(&data[len - 4..len]),
                &data[..len - 4],
                empty,
            ),
            1 => {
                let key_len = slice_u16(&data[len - 2..len]) as usize;
                (
                    EventType::Create,
                    0,
                    &data[len - 2 - key_len..len - 2],
                    &data[..len - 2 - key_len],
                )
            }
            2 => {
                let key_len = slice_u16(&data[len - 6..len - 4]) as usize;
                (
                    EventType::Update,
                    slice_u32(&data[len - 4..len]),
                    &data[len - key_len - 6..len - 6],
                    &data[..len - key_len - 6],
                )
            }
            _ => panic!("decode has err type:{}", data[len]),
        }
    }
}

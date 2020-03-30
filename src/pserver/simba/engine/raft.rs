// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.
use crate::pserver::simba::simba::Simba;
use crate::util::{coding::*, config, entity::*, error::*};

use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::util::entity::Partition;
use jimraft::{
    error::RResult, raft::LogReader, CmdResult, ConfigChange, NodeResolver, NodeResolverCallback,
    Peer, PeerType, Raft, RaftOptions, RaftServer, RaftServerOptions, Snapshot, StateMachine,
    StateMachineCallback,
};

use crate::client::meta_client::MetaClient;
use log::{error, info, warn};
use std::boxed::Box;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;

pub struct RaftEngine {
    base: Arc<BaseEngine>,
    pub simba: Arc<RwLock<Simba>>,
    pub raft: Arc<Raft>,
}

impl Deref for RaftEngine {
    type Target = BaseEngine;
    fn deref<'a>(&'a self) -> &'a BaseEngine {
        &self.base
    }
}

impl RaftEngine {
    pub fn new(base: Arc<BaseEngine>, simba: Arc<RwLock<Simba>>) -> Self {
        let raft: Arc<Raft> =
            create_raft(simba.clone(), base.clone(), simba.read().unwrap().server_id);
        Self {
            base: base.clone(),
            simba: simba.clone(),
            raft: raft,
        }
    }

    pub fn append<'a, T: 'static>(&self, event: impl Event, callback: T) -> ASResult<()>
    where
        T: AppendCallback + 'a,
    {
        let faced = AppendCallbackFaced {
            target: Box::new(callback),
        };
        unsafe {
            self.raft.propose(
                &LogEvent::from_event(event),
                1,
                mem::transmute(Box::new(faced)),
            );
        }
        Ok(())
    }

    pub fn begin_read_log(&self, start_index: u64) -> ASResult<LogReader> {
        match self.raft.begin_read_log(start_index) {
            Ok(logger) => return Ok(logger),
            Err(e) => return Err(err_str_box("get raft logger error")),
        }
    }

    // fetch raft log entries since start_index
    // std::unique_ptr<LogReader> ReadLog(uint64_t start_index) = 0;
}

pub trait AppendCallback {
    fn call(&self);
}

pub struct AppendCallbackFaced {
    pub target: Box<dyn AppendCallback>,
}

impl AppendCallbackFaced {
    pub fn call(&self) {
        self.target.call();
    }
}

pub enum EventType {
    Put = 1,
    Delete = 2,
}

pub trait Event {
    fn serialize(&mut self) -> Vec<u8>;
    fn get_type(&self) -> EventType;
}

pub struct DelEvent {
    pub k: Vec<u8>,
}

impl DelEvent {
    pub fn de_serialize(data: Vec<u8>) -> Self {
        Self { k: data }
    }
}

impl Event for DelEvent {
    fn get_type(&self) -> EventType {
        EventType::Delete
    }

    fn serialize(&mut self) -> Vec<u8> {
        let mut result: Vec<u8> = vec![];
        result.append(u32_slice(self.k.len() as u32).to_vec().as_mut());
        result.append(self.k.as_mut());
        result
    }
}

pub struct PutEvent {
    pub k: Vec<u8>,
    pub v: Vec<u8>,
}

impl PutEvent {
    pub fn de_serialize(data: Vec<u8>) -> Self {
        let (k_len, right) = data.split_at(4);
        let (k_, right) = right.split_at(slice_u32(k_len) as usize);
        let (v_len, v_) = right.split_at(4);

        Self {
            k: k_.to_vec(),
            v: v_.to_vec(),
        }
    }
}

impl Event for PutEvent {
    fn get_type(&self) -> EventType {
        EventType::Put
    }
    fn serialize(&mut self) -> Vec<u8> {
        let mut result: Vec<u8> = vec![];
        result.append(u32_slice(self.k.len() as u32).to_vec().as_mut());
        result.append(self.k.as_mut());
        result.append(u32_slice(self.v.len() as u32).to_vec().as_mut());
        result.append(self.v.as_mut());
        result
    }
}

pub struct LogEvent {}

impl LogEvent {
    pub fn from_event(mut event: impl Event) -> Vec<u8> {
        let mut result = vec![];
        result.push(event.get_type() as u8);
        result.append(event.serialize().as_mut());
        result
    }

    pub fn to_event(data: Vec<u8>) -> ASResult<Box<dyn Event>> {
        let (event_type, payload) = data.split_at(1);
        if event_type[0] == EventType::Delete as u8 {
            return Ok(Box::new(DelEvent::de_serialize(payload.to_vec())));
        } else if event_type[0] == EventType::Delete as u8 {
            return Ok(Box::new(PutEvent::de_serialize(payload.to_vec())));
        } else {
            return Err(err_str_box("unrecognized log event"));
        }
    }
}

pub struct RaftServerFactory {
    raft_server: Arc<RaftServer>,
}

impl RaftServerFactory {
    pub fn get_instance(conf: Arc<config::Config>, node_id: u64) -> Arc<RaftServer> {
        static mut SERVER: Option<Arc<RaftServer>> = None;
        unsafe {
            // use mut static variable in Rust is unsafe
            SERVER
                .get_or_insert_with(|| {
                    // instance singleton object
                    Arc::new(create_raft_server(conf, node_id))
                })
                .clone()
        }
    }
}

struct SimpleStateMachine {
    pub engine: Arc<BaseEngine>,
    pub simba: Arc<RwLock<Simba>>,
    pub persisted: u64,
    pub peer_id: u64,
}

impl StateMachine for SimpleStateMachine {
    fn apply(&mut self, result: &CmdResult) -> RResult<()> {
        self.persisted = result.index;
        self.engine.set_sn_if_max(result.index);
        unsafe {
            let cb = &mut *(result.tag as *mut AppendCallbackFaced);
            cb.call();
        }
        Ok(())
    }

    fn apply_member_change(&self, _conf: *const ConfigChange, _member: u64) -> RResult<()> {
        Ok(())
    }

    fn persist_applied(&self) -> RResult<u64> {
        Ok(self.persisted)
    }

    fn on_leader_change(&self, leader: u64, term: u64) {
        //on leader change
        //self.simba.role_change(leader == self.peer_id);
    }

    fn get_snapshot(&self) -> RResult<Snapshot> {
        unimplemented!()
    }

    fn apply_snapshot_start(&self, _context: Vec<u8>, index: u64) -> RResult<()> {
        Ok(())
    }

    fn apply_snapshot_data(&self, datas: Vec<Vec<u8>>) -> RResult<()> {
        Ok(())
    }

    fn apply_snapshot_finish(&mut self, index: u64) -> RResult<()> {
        Ok(())
    }

    fn apply_read_index(&self, _cmd: Vec<u8>, index: u16) -> RResult<()> {
        Ok(())
    }
}

pub struct SimpleNodeResolver {
    pub meta_client: Arc<MetaClient>,
}

impl SimpleNodeResolver {
    pub fn new(conf: Arc<config::Config>) -> Self {
        Self {
            meta_client: Arc::new(MetaClient::new(conf)),
        }
    }
}
use futures::executor::block_on;
impl NodeResolver for SimpleNodeResolver {
    fn get_node_address(&self, node_id: u64) -> RResult<String> {
        match block_on(self.meta_client.get_server_addr_by_id(node_id)) {
            Ok(addr) => Ok(addr),
            Err(_) => Err(jimraft::error::err_str(
                &format!("get node address error,node id[{}] ", node_id).as_str(),
            )),
        }
    }
}

fn create_raft_server(conf: Arc<config::Config>, node_id: u64) -> RaftServer {
    let server_ops: RaftServerOptions = RaftServerOptions::new();
    let nr_callback: NodeResolverCallback = NodeResolverCallback {
        target: Box::new(SimpleNodeResolver::new(conf.clone())),
    };

    server_ops.set_node_resolver(nr_callback);
    server_ops.set_node_id(node_id);
    server_ops.set_tick_interval(conf.ps.rs.tick_interval);
    server_ops.set_election_tick(conf.ps.rs.election_tick);
    server_ops.set_transport_inprocess_use(conf.ps.rs.transport_inprocess_use);
    RaftServer::new(server_ops)
}

fn create_raft(simba: Arc<RwLock<Simba>>, base: Arc<BaseEngine>, node_id: u64) -> Arc<Raft> {
    let partition = simba.read().unwrap().partition.clone();
    let conf = simba.read().unwrap().conf.clone();
    let options = RaftOptions::new();
    let (current_peer_id, peers) = create_peers(&partition, node_id);
    let callback: StateMachineCallback = StateMachineCallback {
        target: Box::new(SimpleStateMachine {
            engine: base.clone(),
            simba: simba.clone(),
            persisted: 0,
            peer_id: current_peer_id,
        }),
    };

    options.set_id(generate_raft_id(partition.collection_id, partition.id));
    options.set_peers(peers);
    options.set_state_machine(callback);
    options.set_use_memoray_storage(true);

    let raft_server: Arc<RaftServer> = RaftServerFactory::get_instance(conf.clone(), node_id);
    let raft: Raft = raft_server.create_raft(&options).unwrap();
    Arc::new(raft)
}

fn generate_raft_id(collection_id: u32, partition_id: u32) -> u64 {
    merge_u32(collection_id, partition_id)
}

fn create_peers(partition: &Partition, node_id: u64) -> (u64, Vec<Peer>) {
    let mut current_peer_id = 0;
    let mut peers: Vec<Peer> = vec![];
    for replica in &partition.replicas {
        if replica.node == node_id {
            current_peer_id = replica.peer;
        }
        let mut peer_type = PeerType::NORMAL;
        match replica.replica_type {
            ReplicaType::LEARNER => peer_type = PeerType::LEARNER,
            ReplicaType::NORMAL => peer_type = PeerType::NORMAL,
        }

        let peer: Peer = Peer {
            type_: peer_type,
            node_id: replica.node,
            id: replica.peer,
        };

        peers.push(peer);
    }

    (current_peer_id, peers)
}

impl Engine for RaftEngine {
    fn flush(&self, pre_sn: u64) -> Option<u64> {
        None
    }

    fn release(&self) {
        //TODO
    }
}

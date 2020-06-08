use crate::client::meta_client::MetaClient;
use crate::pserver::raft::state_machine::MemberChange;
use crate::pserver::raft::state_machine::*;
use crate::util::entity::Partition;
use crate::util::{coding::*, config, entity::*, error::*};
use crate::*;
use async_std::task;
use jimraft::{
    error::RResult, raft::LogReader, NodeResolver, NodeResolverCallback, Peer, PeerType, Raft,
    RaftOptions, RaftServer, RaftServerOptions, StateMachineCallback,
};
use std::boxed::Box;
use std::collections::HashMap;
use std::mem;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

pub struct JimRaftServer {
    _conf: Arc<config::Config>,
    pub node_id: u64,
    raft_server: RaftServer,
}

impl JimRaftServer {
    pub fn get_instance(conf: Arc<config::Config>, node_id: u64) -> Arc<JimRaftServer> {
        static mut SERVER: Option<Arc<JimRaftServer>> = None;

        unsafe {
            // use mut static variable in Rust is unsafe
            SERVER
                .get_or_insert_with(|| {
                    // instance singleton object
                    Arc::new(Self::create_raft_server(conf, node_id))
                })
                .clone()
        }
    }

    fn create_raft_server(conf: Arc<config::Config>, node_id: u64) -> Self {
        let server_ops: RaftServerOptions = RaftServerOptions::new();
        let nr_callback: NodeResolverCallback = NodeResolverCallback {
            target: Box::new(SimpleNodeResolver::new(conf.clone())),
        };
        server_ops.set_node_resolver(nr_callback);
        server_ops.set_node_id(node_id);
        server_ops.set_tick_interval(conf.ps.rs.tick_interval);
        server_ops.set_election_tick(conf.ps.rs.election_tick);
        server_ops.set_transport_inprocess_use(conf.ps.rs.transport_inprocess_use);
        Self {
            _conf: conf,
            node_id: node_id,
            raft_server: RaftServer::new(server_ops),
        }
    }

    pub fn create_raft(
        &self,
        conf: Arc<config::Config>,
        partition: Arc<Partition>,
        sender: Arc<Mutex<Sender<MemberChange>>>,
    ) -> ASResult<Raft> {
        let (cid, pid) = (partition.collection_id, partition.id);
        let options = RaftOptions::new();
        let (current_peer_id, peers) = self.create_peers(partition);
        let callback: StateMachineCallback = StateMachineCallback {
            target: Box::new(SimpleStateMachine {
                persisted: 0,
                peer_id: current_peer_id,
                collection_id: cid,
                partition_id: pid,
                sender: sender,
            }),
        };
        options.set_id(merge_u32(cid, pid));
        options.set_peers(peers);
        options.set_state_machine(callback);
        options.set_use_memoray_storage(false);
        let base: String = conf.ps.clone().raft.clone().storage_path.unwrap();
        let path = base + "/" + cid.to_string().as_str() + "/" + pid.to_string().as_str();
        options.set_storage_path(path.as_str());

        convert(self.raft_server.create_raft(&options))
    }

    fn create_peers(&self, partition: Arc<Partition>) -> (u64, Vec<Peer>) {
        let mut current_peer_id = 0;
        let mut peers: Vec<Peer> = vec![];
        for replica in partition.replicas.iter() {
            if replica.node == self.node_id as u32 {
                current_peer_id = replica.peer;
            }

            let peer_type = match replica.replica_type {
                ReplicaType::LEARNER => PeerType::LEARNER,
                ReplicaType::NORMAL => PeerType::NORMAL,
            };

            let peer: Peer = Peer {
                type_: peer_type,
                node_id: replica.node as u64,
                id: replica.peer,
            };
            peers.push(peer);
        }
        (current_peer_id, peers)
    }
}

pub struct SimpleNodeResolver {
    pub meta_client: Arc<MetaClient>,
    pub cache: RwLock<HashMap<u64, String>>,
}

impl SimpleNodeResolver {
    pub fn new(conf: Arc<config::Config>) -> Self {
        Self {
            meta_client: Arc::new(MetaClient::new(conf)),
            cache: RwLock::new(HashMap::new()),
        }
    }
}
impl NodeResolver for SimpleNodeResolver {
    fn get_node_address(&self, node_id: u64) -> RResult<String> {
        if let Some(addr) = self.cache.read().unwrap().get(&node_id) {
            return Ok(addr.to_string());
        }

        let addr = || -> RResult<String> {
            match task::block_on(self.meta_client.get_server_addr_by_id(node_id)) {
                Ok(addr) => {
                    let parts: Vec<&str> = addr.split("_").collect();
                    Ok(parts[0].to_string())
                }
                Err(_) => Err(jimraft::error::err_str(
                    &format!("get node address error,node id[{}] ", node_id).as_str(),
                )),
            }
        };

        let v = self
            .cache
            .write()
            .unwrap()
            .entry(node_id)
            .or_insert(addr()?)
            .to_string();
        return Ok(v);
    }
}

pub struct RaftEngine {
    pub collection: Arc<Collection>,
    pub partition: Arc<Partition>,
    pub raft: Raft,
}

impl RaftEngine {
    pub fn new(collection: Arc<Collection>, partition: Arc<Partition>, raft: Raft) -> Self {
        Self {
            collection: collection,
            partition: partition,
            raft: raft,
        }
    }

    pub fn append<'a, T: 'static>(&self, event: Event, callback: T) -> ASResult<()>
    where
        T: AppendCallback + 'a,
    {
        let faced = AppendCallbackFaced {
            target: Box::new(callback),
        };
        unsafe {
            self.raft.propose(
                &EventCodec::encode(event),
                1,
                mem::transmute(Box::new(faced)),
            );
        }
        Ok(())
    }

    pub fn begin_read_log(&self, start_index: u64) -> ASResult<LogReader> {
        match self.raft.begin_read_log(start_index) {
            Ok(logger) => return Ok(logger),
            Err(e) => return result_def!("get raft logger failure. error:[{}]", e),
        }
    }
}

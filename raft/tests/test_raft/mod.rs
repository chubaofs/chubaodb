#![allow(dead_code)]

mod mem_storage;
mod test_network;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mem_storage::MemoryStorage;
use test_network::TestNetwork;
use tokio::sync::RwLock;
use tracing_subscriber::layer::SubscriberExt;
use xraft::{Config, Metrics, NodeId, Raft, RaftError, Result};

#[derive(Debug, Clone)]
pub enum Action {
    Put(String, i32),
    Delete(String),
}

impl Action {
    pub fn put(key: impl Into<String>, value: i32) -> Self {
        Self::Put(key.into(), value)
    }

    pub fn delete(key: impl Into<String>) -> Self {
        Self::Delete(key.into())
    }
}

pub struct Node {
    pub raft: Raft<(), Action>,
    pub storage: Arc<MemoryStorage>,
}

#[derive(Clone)]
pub struct TestHarness {
    config: Arc<Config>,
    nodes: Arc<RwLock<BTreeMap<NodeId, Node>>>,
    isolated_nodes: Arc<RwLock<BTreeSet<NodeId>>>,
    network: Arc<TestNetwork>,
}

impl Default for TestHarness {
    fn default() -> Self {
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::Layer::default());
        tracing::subscriber::set_global_default(subscriber).unwrap();

        let config = Arc::new(Config::default());
        let network = Arc::new(TestNetwork::default());
        Self {
            config,
            nodes: network.nodes.clone(),
            isolated_nodes: Default::default(),
            network,
        }
    }
}

impl TestHarness {
    pub async fn add_node(&self, id: NodeId) {
        let mut nodes = self.nodes.write().await;
        let storage = Arc::new(MemoryStorage::default());
        let raft = Raft::new(
            format!("node-{}", id),
            id,
            self.config.clone(),
            storage.clone(),
            self.network.clone(),
        )
        .unwrap();
        nodes.insert(id, Node { raft, storage });
    }

    pub async fn initialize(&self) -> Result<()> {
        let nodes = self.nodes.read().await;
        assert!(!nodes.is_empty());
        let raft = &nodes.iter().next().unwrap().1.raft;
        raft.initialize(nodes.keys().map(|id| (*id, ()))).await?;
        Ok(())
    }

    pub async fn add_non_voter(&self, id: NodeId) -> Result<()> {
        let nodes = self.nodes.read().await;
        let mut leader = *nodes.keys().next().unwrap();
        loop {
            let node = nodes.get(&leader).unwrap();
            match node.raft.add_non_voter(id, ()).await {
                Ok(()) => break,
                Err(RaftError::ForwardToLeader(Some(forward_to))) => leader = forward_to,
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub async fn metrics(&self, id: NodeId) -> Result<Metrics<()>> {
        let nodes = self.nodes.read().await;
        let node = nodes.get(&id).unwrap();
        node.raft.metrics().await
    }

    pub async fn isolate_node(&self, id: NodeId) {
        self.isolated_nodes.write().await.insert(id);
    }

    pub async fn restore_node(&self, id: NodeId) {
        self.isolated_nodes.write().await.remove(&id);
    }

    pub async fn write(&self, action: Action) -> Result<()> {
        let nodes = self.nodes.read().await;
        let mut leader = *nodes.keys().next().unwrap();
        loop {
            let node = nodes.get(&leader).unwrap();
            match node.raft.client_write(action.clone()).await {
                Ok(()) => break,
                Err(RaftError::ForwardToLeader(Some(forward_to))) => leader = forward_to,
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub async fn read(&self, key: impl AsRef<str>) -> Result<Option<i32>> {
        let nodes = self.nodes.read().await;
        let mut leader = *nodes.keys().next().unwrap();
        loop {
            let node = nodes.get(&leader).unwrap();
            match node.raft.client_read().await {
                Ok(()) => return Ok(node.storage.get(key)),
                Err(RaftError::ForwardToLeader(Some(forward_to))) => leader = forward_to,
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn read_from(&self, target: NodeId, key: impl AsRef<str>) -> Result<Option<i32>> {
        let nodes = self.nodes.read().await;
        let node = nodes.get(&target).unwrap();
        Ok(node.storage.get(key))
    }
}

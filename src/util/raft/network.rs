use alaya_protocol::raft::{Entry as RaftEntry, WriteAction, WriteActions};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::RwLock;
use xraft::{
    AppendEntriesRequest, AppendEntriesResponse, Entry, Network, NetworkResult, NodeId, Raft,
    VoteRequest, VoteResponse,
};

pub struct Member {
    pub id: u64,
    pub addr: String,
    pub raft: Arc<Raft<NodeId, WriteActions>>,
}

#[derive(Default)]
pub struct RaftNetwork {
    pub nodes: Arc<RwLock<BTreeMap<NodeId, Member>>>,
    pub isolated_nodes: Arc<RwLock<BTreeSet<NodeId>>>,
}

#[async_trait::async_trait]
impl Network<NodeId, WriteActions> for RaftNetwork {
    async fn vote(
        &self,
        target: u64,
        _target_info: &NodeId,
        req: VoteRequest,
    ) -> NetworkResult<VoteResponse> {
        let nodes = self.nodes.read().await;
        let node = nodes.get(&target).unwrap();
        anyhow::ensure!(
            !self.isolated_nodes.read().await.contains(&target),
            "Node '{}' is isolated",
            target
        );
        Ok(node.raft.vote(req).await?)
    }

    async fn append_entries(
        &self,
        target: u64,
        _target_info: &NodeId,
        req: AppendEntriesRequest<NodeId, WriteActions>,
    ) -> NetworkResult<AppendEntriesResponse> {
        let nodes = self.nodes.read().await;
        let node = nodes.get(&target).unwrap();
        anyhow::ensure!(
            !self.isolated_nodes.read().await.contains(&target),
            "Node '{}' is isolated",
            target
        );
        Ok(node.raft.append_entries(req).await?)
    }
}

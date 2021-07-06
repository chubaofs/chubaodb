use alaya_protocol::raft::Entry as RaftEntry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::RwLock;
use xraft::{
    AppendEntriesRequest, AppendEntriesResponse, Entry, Network, NetworkResult, NodeId,
    VoteRequest, VoteResponse,
};

#[derive(Default)]
pub struct RaftNetwork {
    pub nodes: Arc<RwLock<BTreeMap<NodeId, Member>>>,
    pub isolated_nodes: Arc<RwLock<BTreeSet<Member>>>,
}

#[async_trait::async_trait]
impl Network<N, Entry<Member, RaftEntry>> for RaftNetwork {
    async fn vote(
        &self,
        target: u64,
        _target_info: &(),
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
        _target_info: &(),
        req: AppendEntriesRequest<Member, Entry<Member, RaftEntry>>,
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

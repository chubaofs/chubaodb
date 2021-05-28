use serde::{Deserialize, Serialize};

use crate::{Entry, LogIndex, NodeId, TermId};

pub type NetworkResult<T> = anyhow::Result<T>;

#[derive(Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: TermId,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: LogIndex,
}

#[derive(Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesRequest<N, D> {
    pub term: TermId,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: TermId,
    pub leader_commit: LogIndex,
    pub entries: Vec<Entry<N, D>>,
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: TermId,
    pub success: bool,
}

#[async_trait::async_trait]
pub trait Network<N, D>: Send + Sync + Unpin {
    async fn vote(
        &self,
        target: NodeId,
        target_info: &N,
        req: VoteRequest,
    ) -> NetworkResult<VoteResponse>;

    async fn append_entries(
        &self,
        target: NodeId,
        target_info: &N,
        req: AppendEntriesRequest<N, D>,
    ) -> NetworkResult<AppendEntriesResponse>;
}

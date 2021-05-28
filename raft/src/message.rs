use tokio::sync::oneshot;

use crate::{
    AppendEntriesRequest, AppendEntriesResponse, Metrics, NodeId, Result, VoteRequest, VoteResponse,
};

pub enum Message<N, D> {
    Initialize {
        members: Vec<(NodeId, N)>,
        reply: oneshot::Sender<Result<()>>,
    },
    Vote {
        req: VoteRequest,
        reply: oneshot::Sender<Result<VoteResponse>>,
    },
    AppendEntries {
        req: AppendEntriesRequest<N, D>,
        reply: oneshot::Sender<Result<AppendEntriesResponse>>,
    },
    ClientWrite {
        action: D,
        reply: oneshot::Sender<Result<()>>,
    },
    ClientRead {
        reply: oneshot::Sender<Result<()>>,
    },
    AddNode {
        id: NodeId,
        info: N,
        reply: oneshot::Sender<Result<()>>,
    },
    RemoveNode {
        id: NodeId,
        reply: oneshot::Sender<Result<()>>,
    },
    Metrics {
        reply: oneshot::Sender<Result<Metrics<N>>>,
    },
    Shutdown,
}

use std::sync::Arc;

use alaya_protocol::{meta_raft::*, raft::*};
use anyhow::Result;
use tonic::{Request, Response, Status};
use xraft::Raft;
use crate::meta::store::HARepository;

pub struct MetaRaftService {
    raft: Arc<Raft<Vec<u8>, Vec<u8>>>,
    store: HARepository,
}

impl MetaRaftService {
    pub fn new(raft: Arc<Raft<Vec<u8>, Vec<u8>>, store HARepository) -> Self {
        Self { raft, store }
    }
}

#[async_trait::async_trait]
impl meta_raft_server::MetaRaft for MetaRaftService {
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        Ok(Response::new(
            self.raft
                .vote(request.into_inner())
                .await
                .map_err(|err| Status::internal(err.to_string()))?,
        ))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        Ok(Response::new(
            self.raft
                .append_entries(request.into_inner())
                .await
                .map_err(|err| Status::internal(err.to_string()))?,
        ))
    }

    async fn metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        let metrics = self
            .raft
            .metrics()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(GetMetricsResponse {
            metrics: Some(metrics),
        }))
    }
}

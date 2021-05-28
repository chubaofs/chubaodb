// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
use crate::util::error::*;
use crate::*;
use alaya_protocol::pserver::{rpc_client::RpcClient, *};
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
#[derive(Default)]
pub struct PartitionClient {
    pub addr: String,
    pub collection_id: u32,
    pub partition_id: u32,
    pub slot: u32,
}

impl PartitionClient {
    pub fn new(addr: String) -> Self {
        PartitionClient {
            addr: addr,
            ..Default::default()
        }
    }
}

//for ps
impl PartitionClient {
    pub fn addr(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub async fn write(
        &self,
        mut rpc_client: RpcClient<Channel>,
        req: WriteDocumentRequest,
    ) -> ASResult<GeneralResponse> {
        let resp = conver(rpc_client.write(Request::new(req)).await)?.into_inner();
        result_obj_code!(resp)
    }

    pub async fn get(
        &self,
        mut rpc_client: RpcClient<Channel>,
        req: GetDocumentRequest,
    ) -> ASResult<DocumentResponse> {
        let resp = conver(rpc_client.get(Request::new(req)).await)?.into_inner();
        result_obj_code!(resp)
    }
}

//for master
impl PartitionClient {
    pub async fn status(&self, req: GeneralRequest) -> ASResult<GeneralResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        let resp = rpc_client.status(Request::new(req)).await?.into_inner();
        result_obj_code!(resp)
    }

    pub async fn load_or_create_partition(
        &self,
        req: PartitionRequest,
    ) -> ASResult<GeneralResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        let resp = rpc_client
            .load_partition(Request::new(req))
            .await?
            .into_inner();
        result_obj_code!(resp)
    }

    //offload partition , if partition not exist it not return err
    pub async fn offload_partition(&self, req: PartitionRequest) -> ASResult<GeneralResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        let resp = rpc_client
            .offload_partition(Request::new(req))
            .await?
            .into_inner();
        result_obj_code!(resp)
    }
}

pub struct MultiplePartitionClient {
    pub addr: String,
    pub collection_partition_ids: Vec<u64>,
}

//for ps
impl MultiplePartitionClient {
    pub fn new(addr: String) -> Self {
        Self {
            addr: addr,
            collection_partition_ids: Vec::new(),
        }
    }

    pub async fn search(self, query: QueryRequest) -> ASResult<SearchDocumentResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);

        let resp = rpc_client.search(Request::new(query)).await?;

        Ok(resp.into_inner())
    }

    pub async fn agg(self, query: QueryRequest) -> ASResult<AggregationResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);

        let resp = rpc_client.agg(Request::new(query)).await?;

        Ok(resp.into_inner())
    }

    pub async fn count(&self) -> ASResult<CountDocumentResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        let resp = rpc_client
            .count(Request::new(CountDocumentRequest {
                cpids: self.collection_partition_ids.clone(),
            }))
            .await?
            .into_inner();

        result_obj_code!(resp)
    }

    fn addr(&self) -> String {
        format!("http://{}", self.addr)
    }
}

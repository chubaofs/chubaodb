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
use crate::pserverpb::{rpc_client::RpcClient, *};
use crate::util::error::*;
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
        match rpc_client.write(Request::new(req)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                let code = resp.code as u16;
                if code != SUCCESS {
                    Err(err_code_box(code, resp.message))
                } else {
                    Ok(resp)
                }
            }
            Err(e) => Err(err_str_box(e.message())),
        }
    }

    pub async fn get(
        &self,
        mut rpc_client: RpcClient<Channel>,
        req: GetDocumentRequest,
    ) -> ASResult<DocumentResponse> {
        match rpc_client.get(Request::new(req)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                let code = resp.code as u16;
                if code != SUCCESS {
                    Err(err_code_box(code, resp.message))
                } else {
                    Ok(resp)
                }
            }
            Err(e) => Err(err_str_box(e.message())),
        }
    }
}

//for master
impl PartitionClient {
    pub async fn status(&self, req: GeneralRequest) -> ASResult<GeneralResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        match rpc_client.status(Request::new(req)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                let code = resp.code as u16;
                if code != SUCCESS {
                    Err(err_code_box(code, resp.message))
                } else {
                    Ok(resp)
                }
            }
            Err(e) => Err(err_str_box(e.message())),
        }
    }

    pub async fn command(&self, req: CommandRequest) -> ASResult<CommandResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        match rpc_client.command(Request::new(req)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                let code = resp.code as u16;
                if code != SUCCESS {
                    Err(err_code_box(code, resp.message))
                } else {
                    Ok(resp)
                }
            }
            Err(e) => Err(err_str_box(e.message())),
        }
    }

    pub async fn load_or_create_partition(
        &self,
        req: PartitionRequest,
    ) -> ASResult<GeneralResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        match rpc_client.load_partition(Request::new(req)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                let code = resp.code as u16;
                if code != SUCCESS {
                    Err(err_code_box(code, resp.message))
                } else {
                    Ok(resp)
                }
            }
            Err(e) => Err(err_str_box(e.message())),
        }
    }

    //offload partition , if partition not exist it not return err
    pub async fn offload_partition(&self, req: PartitionRequest) -> ASResult<GeneralResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        match rpc_client.offload_partition(Request::new(req)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                let code = resp.code as u16;
                if code != SUCCESS {
                    Err(err_code_box(code, resp.message))
                } else {
                    Ok(resp)
                }
            }
            Err(e) => Err(err_str_box(e.message())),
        }
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

    pub async fn search(
        self,
        query: String,
        def_fields: Vec<String>,
        size: u32,
    ) -> ASResult<SearchDocumentResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        match rpc_client
            .search(Request::new(SearchDocumentRequest {
                cpids: self.collection_partition_ids,
                query: query,
                filter: String::default(),
                def_fields: def_fields,
                vector_querys: Vec::default(),
                size: size,
            }))
            .await
        {
            Ok(resp) => Ok(resp.into_inner()),
            Err(e) => Err(err_str_box(e.message())),
        }
    }

    pub async fn count(&self) -> ASResult<CountDocumentResponse> {
        let mut rpc_client = RpcClient::new(Endpoint::from_shared(self.addr())?.connect().await?);
        match rpc_client
            .count(Request::new(CountDocumentRequest {
                cpids: self.collection_partition_ids.clone(),
            }))
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                let code = resp.code as u16;
                if code != SUCCESS {
                    Err(err_code_box(code, resp.message))
                } else {
                    Ok(resp)
                }
            }
            Err(e) => Err(err_str_box(e.message())),
        }
    }

    fn addr(&self) -> String {
        format!("http://{}", self.addr)
    }
}

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
use crate::pserver::service::PartitionService;
use crate::pserverpb::{
    rpc_server::{Rpc, RpcServer},
    *,
};
use crate::util::entity::*;
use crate::util::{config, error::*};
use log::{error, info};
use std::error::Error;
use std::sync::{mpsc::Sender, Arc};
use std::time;
use tokio;
use tonic::{transport::Server, Request, Response, Status};

#[tokio::main]
pub async fn start(tx: Sender<String>, conf: Arc<config::Config>) -> Result<(), Box<dyn Error>> {
    //if ps got ip is empty to got it by master
    let mut config = (*conf).clone();

    if conf.global.ip == "" {
        let m_client = crate::client::meta_client::MetaClient::new(conf.clone());
        loop {
            match m_client.my_ip().await {
                Ok(ip) => {
                    info!("got my ip:{} from master", ip);
                    config.global.ip = ip;
                    break;
                }
                Err(e) => {
                    error!("got ip from master has err:{:?}", e);
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
    }

    let conf = Arc::new(config);

    let mut ps = PartitionService::new(conf.clone());

    let now = time::SystemTime::now();
    while let Err(err) = ps.init().await {
        error!("partition init has err:{:?} it will try again!", err);
        std::thread::sleep(time::Duration::from_secs(1));
    }
    info!(
        "init pserver OK use time:{:?}",
        time::SystemTime::now().duration_since(now)
    );

    //start heartbeat  TODO..........

    let addr = format!("{}:{}", conf.global.ip, conf.ps.rpc_port)
        .parse()
        .unwrap();

    let rpc_service = RpcServer::new(RPCService::new(ps));

    let _ = Server::builder()
        .add_service(rpc_service)
        .serve(addr)
        .await?;
    let _ = tx.send(String::from("pserver over"));
    Ok(())
}

pub struct RPCService {
    service: PartitionService,
}

impl RPCService {
    fn new(ps: PartitionService) -> Self {
        RPCService { service: ps }
    }
}

#[tonic::async_trait]
impl Rpc for RPCService {
    async fn write(
        &self,
        request: Request<WriteDocumentRequest>,
    ) -> Result<Response<GeneralResponse>, Status> {
        let result = match self.service.write(request.into_inner()).await {
            Ok(gr) => gr,
            Err(e) => {
                let e = cast_to_err(e);
                GeneralResponse {
                    code: e.0 as i32,
                    message: format!("write document err:{}", e.1),
                }
            }
        };

        Ok(Response::new(result))
    }

    async fn get(
        &self,
        request: Request<GetDocumentRequest>,
    ) -> Result<Response<DocumentResponse>, Status> {
        let result = match self.service.get(request.into_inner()) {
            Ok(gr) => gr,
            Err(e) => {
                let e = cast_to_err(e);
                DocumentResponse {
                    code: e.0 as i32,
                    message: format!("get document err:{}", e.1),
                    doc: Vec::default(),
                }
            }
        };
        Ok(Response::new(result))
    }

    async fn search(
        &self,
        request: Request<SearchDocumentRequest>,
    ) -> Result<Response<SearchDocumentResponse>, Status> {
        let result = match self.service.search(request.into_inner()).await {
            Ok(gr) => gr,
            Err(e) => {
                let e = cast_to_err(e);
                SearchDocumentResponse {
                    code: e.0 as i32,
                    total: 0,
                    hits: vec![],
                    info: Some(SearchInfo {
                        error: 1,
                        success: 0,
                        message: format!("search document err:{}", e.1),
                    }),
                }
            }
        };
        Ok(Response::new(result))
    }

    async fn count(
        &self,
        request: Request<CountDocumentRequest>,
    ) -> Result<Response<CountDocumentResponse>, Status> {
        let result = match self.service.count(request.into_inner()).await {
            Ok(gr) => gr,
            Err(e) => {
                let e = cast_to_err(e);
                CountDocumentResponse {
                    code: e.0 as i32,
                    message: format!("search document err:{}", e.1),
                    ..Default::default()
                }
            }
        };
        Ok(Response::new(result))
    }

    async fn status(
        &self,
        request: Request<GeneralRequest>,
    ) -> Result<Response<GeneralResponse>, Status> {
        let result = match self.service.status(request.into_inner()) {
            Ok(gr) => gr,
            Err(e) => {
                let e = cast_to_err(e);
                GeneralResponse {
                    code: e.0 as i32,
                    message: format!("status err:{}", e.1),
                }
            }
        };
        Ok(Response::new(result))
    }

    async fn load_partition(
        &self,
        request: Request<PartitionRequest>,
    ) -> Result<Response<GeneralResponse>, Status> {
        let req = request.into_inner();
        info!("Start server load_or_create_partition");

        let mut replicas: Vec<Replica> = vec![];
        for rep in req.replicas {
            let mut replica_type: ReplicaType = ReplicaType::NORMAL;
            if rep.replica_type == 1 {
                replica_type = ReplicaType::LEARNER;
            }
            replicas.push(Replica {
                node: rep.node,
                peer: rep.peer,
                replica_type: replica_type,
            });
        }
        let mut result = match self
            .service
            .init_partition(
                req.collection_id,
                req.partition_id,
                replicas,
                req.readonly,
                req.version,
                true,
            )
            .await
        {
            Ok(_) => GeneralResponse {
                code: SUCCESS as i32,
                message: String::from("success"),
            },
            Err(e) => {
                let e = cast_to_err(e);
                GeneralResponse {
                    code: e.0 as i32,
                    message: format!(
                        "load_or_create partiiton in:{} has err:{}",
                        self.service.conf.global.ip, e.1
                    ),
                }
            }
        };

        if let Err(e) = self.service.take_heartbeat().await {
            let e = cast_to_err(e);
            result = GeneralResponse {
                code: e.0 as i32,
                message: format!(
                    "load_or_create partiiton in:{} has err:{}",
                    self.service.conf.global.ip, e.1
                ),
            };
        }

        Ok(Response::new(result))
    }

    async fn offload_partition(
        &self,
        request: Request<PartitionRequest>,
    ) -> Result<Response<GeneralResponse>, Status> {
        let result = match self.service.offload_partition(request.into_inner()) {
            Err(e) => {
                let e = cast_to_err(e);
                GeneralResponse {
                    code: e.0 as i32,
                    message: format!(
                        "offload partiiton in:{} has err:{}",
                        self.service.conf.global.ip, e.1
                    ),
                }
            }
            Ok(rep) => rep,
        };

        if let Err(e) = self.service.take_heartbeat().await {
            let e = cast_to_err(e);
            return Ok(Response::new(GeneralResponse {
                code: e.0 as i32,
                message: format!(
                    "offload partiiton in:{} has err:{}",
                    self.service.conf.global.ip, e.1
                ),
            }));
        };

        Ok(Response::new(result))
    }

    async fn command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        match self.service.command(request.into_inner()) {
            Ok(b) => Ok(Response::new(CommandResponse {
                code: SUCCESS as i32,
                message: String::default(),
                body: b,
            })),
            Err(e) => {
                let e = cast_to_err(e);
                Ok(Response::new(CommandResponse {
                    code: e.0 as i32,
                    message: e.1,
                    body: vec![],
                }))
            }
        }
    }
}

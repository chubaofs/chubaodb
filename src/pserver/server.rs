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
use tonic::{transport::Server, Request, Response, Status};

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
    service: Arc<PartitionService>,
}

impl RPCService {
    fn new(ps: Arc<PartitionService>) -> Self {
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
            Err(e) => e.into(),
        };

        Ok(Response::new(result))
    }

    async fn get(
        &self,
        request: Request<GetDocumentRequest>,
    ) -> Result<Response<DocumentResponse>, Status> {
        let result = match self.service.get(request.into_inner()) {
            Ok(gr) => gr,
            Err(e) => e.into(),
        };
        Ok(Response::new(result))
    }

    async fn search(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<SearchDocumentResponse>, Status> {
        let result = match self.service.search(request.into_inner()).await {
            Ok(gr) => gr,
            Err(e) => e.into(),
        };
        Ok(Response::new(result))
    }

    async fn agg(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<AggregationResponse>, Status> {
        let result = match self.service.agg(request.into_inner()).await {
            Ok(gr) => gr,
            Err(e) => e.into(),
        };
        Ok(Response::new(result))
    }

    async fn count(
        &self,
        request: Request<CountDocumentRequest>,
    ) -> Result<Response<CountDocumentResponse>, Status> {
        let result = match self.service.count(request.into_inner()).await {
            Ok(v) => v,
            Err(e) => e.into(),
        };
        Ok(Response::new(result))
    }

    async fn status(
        &self,
        request: Request<GeneralRequest>,
    ) -> Result<Response<GeneralResponse>, Status> {
        let result = match self.service.status(request.into_inner()).into() {
            Ok(v) => v,
            Err(e) => e.into(),
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
                node_id: rep.node,
                replica_type: replica_type,
            });
        }
        let result = match self
            .service
            .init_partition(
                req.collection_id,
                req.partition_id,
                replicas,
                req.readonly,
                req.version,
            )
            .await
        {
            Ok(_) => make_general_success(),
            Err(e) => e.into(),
        };

        if let Err(e) = self.service.take_heartbeat().await {
            return Ok(Response::new(e.into()));
        }

        Ok(Response::new(result))
    }

    async fn offload_partition(
        &self,
        request: Request<PartitionRequest>,
    ) -> Result<Response<GeneralResponse>, Status> {
        if let Err(e) = self.service.offload_partition(request.into_inner()) {
            return Ok(Response::new(e.into()));
        };

        let rep = match self.service.take_heartbeat().await {
            Ok(_) => make_general_success(),
            Err(e) => e.into(),
        };

        Ok(Response::new(rep))
    }
}

fn make_general_success() -> GeneralResponse {
    GeneralResponse {
        code: Code::Success as i32,
        message: String::from("success"),
    }
}

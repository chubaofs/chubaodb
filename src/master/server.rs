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
use crate::master::cmd::*;
use crate::master::service::MasterService;
use crate::util::{config, entity::*, error::*};
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use log::{error, info, warn};
use serde::Serialize;
use serde_json::json;
use std::sync::{mpsc::Sender, Arc};

#[actix_rt::main]
pub async fn start(tx: Sender<String>, conf: Arc<config::Config>) -> std::io::Result<()> {
    let arc_client = Arc::new(
        MasterService::new(conf.clone()).expect(format!("master service init err").as_str()),
    );

    match arc_client.start() {
        Ok(_) => {}
        Err(e) => {
            error!("master service start error. {}", e.to_string());
        }
    }

    let http_port = match conf.self_master() {
        Some(m) => m.http_port,
        None => panic!("self not set in config "),
    };

    info!("master listening on http://0.0.0.0:{}", http_port);
    HttpServer::new(move || {
        App::new()
            .data(arc_client.clone())
            //admin handler
            .route("/", web::get().to(domain))
            .route("/my_ip", web::get().to(my_ip))
            //zone handler
            .route("/zone/create", web::post().to(create_zone))
            .route("/zone/list", web::get().to(list_zones))
            //pserver handler
            .route("/pserver/put", web::post().to(update_pserver))
            .route("/pserver/list", web::get().to(list_pservers))
            .route("/pserver/heartbeat", web::post().to(heartbeat))
            //collection handler
            .route("/collection/create", web::post().to(create_collection))
            .route(
                "/collection/delete/{collection_name}",
                web::delete().to(del_collection),
            )
            .route(
                "/collection/get/{collection_name}",
                web::get().to(get_collection),
            )
            .route(
                "/collection/get_by_id/{collection_id}",
                web::get().to(get_collection_by_id),
            )
            .route("/collection/list", web::get().to(list_collections))
            //collection partition handler
            .route(
                "/partition/get/{collection_id}/{partition_id}",
                web::get().to(get_partition),
            )
            .route(
                "/collection/partition/list/{collection_name}",
                web::get().to(list_partitions),
            )
            .route(
                "/collection/partition/update",
                web::post().to(update_partition),
            )
            .route(
                "/collection/partition/transfer",
                web::post().to(transfer_partition),
            )
    })
    .bind(format!("0.0.0.0:{}", http_port))?
    .run()
    .await
    .unwrap();

    let _ = tx.send(String::from("master has over"));

    Ok(())
}

async fn domain() -> HttpResponse {
    success_response(json!({
        "anyindex":"master runing",
        "version":config::VERSION,
        "git_version": config::GIT_VERSION,
    }))
}

async fn my_ip(req: HttpRequest) -> HttpResponse {
    let remote = req
        .connection_info()
        .remote()
        .to_owned()
        .unwrap()
        .to_string();
    let addr: Vec<&str> = remote.split(":").collect();

    success_response(json!({
        "ip":addr[0],
    }))
}

async fn create_collection(
    rs: web::Data<Arc<MasterService>>,
    info: web::Json<Collection>,
) -> HttpResponse {
    if info.name.is_none() {
        info!("collection name is none");
        return HttpResponse::build(http_code(INTERNAL_ERR))
            .body(err_str("collection name is none").to_json());
    }

    let name = info.name.clone().unwrap();
    info!("prepare to create collection with name {}", name);
    match rs.create_collection(info.into_inner()).await {
        Ok(s) => success_response(s),
        Err(e) => {
            error!(
                "create collection failed, collection_name: {}, err: {}",
                name, e
            );
            err_response(e)
        }
    }
}

async fn del_collection(rs: web::Data<Arc<MasterService>>, req: HttpRequest) -> HttpResponse {
    let collection_name: String = req
        .match_info()
        .get("collection_name")
        .unwrap()
        .parse()
        .unwrap();

    info!("prepare to delete collection by name {}", collection_name);
    match rs.del_collection(collection_name.as_str()).await {
        Ok(s) => success_response(json!({
            "collection":collection_name,
            "delete_num":s
        })),
        Err(e) => {
            error!(
                "delete collection failed, collection_name {}, err: {}",
                collection_name,
                e.to_string()
            );
            err_response(e)
        }
    }
}

async fn get_collection_by_id(rs: web::Data<Arc<MasterService>>, req: HttpRequest) -> HttpResponse {
    let collection_id: u32 = req
        .match_info()
        .get("collection_id")
        .unwrap()
        .parse()
        .unwrap();

    info!("prepare to get collection by name {}", collection_id);
    match rs.get_collection_by_id(collection_id) {
        Ok(s) => success_response(s),
        Err(e) => {
            error!(
                "get collection failed, collection_id: {}, err: {}",
                collection_id,
                e.to_string()
            );
            err_response(e)
        }
    }
}

async fn get_collection(rs: web::Data<Arc<MasterService>>, req: HttpRequest) -> HttpResponse {
    let collection_name: String = req
        .match_info()
        .get("collection_name")
        .unwrap()
        .parse()
        .unwrap();

    info!("prepare to get collection by name {}", collection_name);
    match rs.get_collection(&collection_name) {
        Ok(s) => success_response(s),
        Err(e) => {
            error!(
                "get collection failed collection_name: {}, err: {}",
                collection_name,
                e.to_string()
            );
            err_response(e)
        }
    }
}

async fn list_collections(rs: web::Data<Arc<MasterService>>) -> HttpResponse {
    info!("prepare to list collections");
    match rs.list_collections() {
        Ok(s) => success_response(s),
        Err(e) => {
            error!("list collection failed, err: {}", e.to_string());
            err_response(e)
        }
    }
}

async fn update_pserver(
    rs: web::Data<Arc<MasterService>>,
    info: web::Json<PServer>,
) -> HttpResponse {
    info!(
        "prepare to update pserver with address {}, zone_id {}",
        info.addr, info.zone_id
    );
    match rs.update_server(info.into_inner()) {
        Ok(s) => success_response(s),
        Err(e) => {
            error!("update server failed, err: {}", e.to_string());
            err_response(e)
        }
    }
}

async fn list_pservers(rs: web::Data<Arc<MasterService>>) -> HttpResponse {
    info!("prepare to list pservers");
    match rs.list_servers() {
        Ok(s) => success_response(s),
        Err(e) => {
            error!("list pserver failed, err: {}", e.to_string());
            err_response(e)
        }
    }
}

async fn heartbeat(rs: web::Data<Arc<MasterService>>, info: web::Json<PServer>) -> HttpResponse {
    info!(
        "prepare to heartbeat with address {}, zone_id {}",
        info.addr, info.zone_id
    );

    let mut ps = match rs.get_server(info.addr.as_str()) {
        Ok(s) => s,
        Err(e) => {
            error!(
                "get server failed, zone_id:{}, server_addr:{}, err:{}",
                info.zone_id,
                info.addr,
                e.to_string()
            );
            return err_response(e);
        }
    };

    let mut active_ids = Vec::new();

    for wp in ps.write_partitions {
        match rs.get_partition(wp.collection_id, wp.id) {
            Ok(dbc) => {
                if dbc.leader == ps.addr && dbc.version <= wp.version {
                    active_ids.push(dbc);
                } else {
                    warn!(
                        "partition not load because not expected:{:?} found:{:?}",
                        dbc, wp
                    );
                }
            }
            Err(e) => {
                error!(
                    "pserver for collection:{} partition:{} get has err:{:?}",
                    wp.collection_id, wp.id, e
                );
            }
        }
    }

    ps.write_partitions = active_ids;

    success_response(ps)
}

async fn get_partition(rs: web::Data<Arc<MasterService>>, req: HttpRequest) -> HttpResponse {
    let collection_id: u32 = req
        .match_info()
        .get("collection_id")
        .unwrap()
        .parse()
        .unwrap();

    let partition_id: u32 = req
        .match_info()
        .get("partition_id")
        .unwrap()
        .parse()
        .unwrap();

    info!(
        "prepare to get partition by collection ID {}, partition ID {}",
        collection_id, partition_id
    );

    match rs.get_partition(collection_id, partition_id) {
        Ok(s) => success_response(s),
        Err(e) => {
            error!(
                "get partition failed, collection_id:{}, partition_id:{}, err:{}",
                collection_id,
                partition_id,
                e.to_string()
            );
            err_response(e)
        }
    }
}

async fn create_zone(rs: web::Data<Arc<MasterService>>, info: web::Json<Zone>) -> HttpResponse {
    info!(
        "prepare to create zone with id {}, name {}",
        info.id.unwrap(),
        info.name.as_ref().unwrap()
    );
    match rs.create_zone(info.into_inner()) {
        Ok(s) => success_response(s),
        Err(e) => {
            error!("create zone failed, err:{}", e.to_string());
            err_response(e)
        }
    }
}

async fn list_zones(rs: web::Data<Arc<MasterService>>) -> HttpResponse {
    info!("prepare to list zones");
    match rs.list_zones() {
        Ok(s) => success_response(s),
        Err(e) => {
            error!("list zone failed, err:{}", e.to_string());
            err_response(e)
        }
    }
}

async fn list_partitions(rs: web::Data<Arc<MasterService>>, req: HttpRequest) -> HttpResponse {
    let collection_name: String = req
        .match_info()
        .get("collection_name")
        .unwrap()
        .parse()
        .unwrap();

    info!(
        "prepare to list partitions with collection name {}",
        &collection_name
    );

    match rs.list_partitions(&collection_name) {
        Ok(s) => success_response(s),
        Err(e) => {
            error!("list partition failed, err: {}", e.to_string());
            err_response(e)
        }
    }
}

async fn update_partition(
    rs: web::Data<Arc<MasterService>>,
    info: web::Json<Partition>,
) -> HttpResponse {
    info!(
        "prepare to update collection {} partition {}  to {}",
        info.collection_id, info.id, info.leader
    );
    match rs.update_partition(info.into_inner()).await {
        Ok(s) => success_response(s),
        Err(e) => {
            error!("update partition failed, err:{}", e.to_string());
            err_response(e)
        }
    }
}

async fn transfer_partition(
    rs: web::Data<Arc<MasterService>>,
    info: web::Json<PTransfer>,
) -> HttpResponse {
    info!(
        "prepare to transfer collection {} partition {}  to {}",
        info.collection_id, info.partition_id, info.to_server
    );
    match rs.transfer_partition(info.into_inner()).await {
        Ok(s) => success_response(s),
        Err(e) => {
            error!("transfer partition failed, err:{}", e.to_string());
            err_response(e)
        }
    }
}

fn err_response(e: Box<dyn std::error::Error>) -> HttpResponse {
    let e = cast_to_err(e);
    return HttpResponse::build(http_code(e.0))
        .content_type("application/json")
        .body(e.to_json());
}

fn success_response<T: Serialize>(result: T) -> HttpResponse {
    HttpResponse::build(http_code(SUCCESS)).json(result)
}

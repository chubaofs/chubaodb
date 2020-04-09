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
use crate::pserverpb::*;
use crate::router::service::RouterService;
use crate::util::{config, error::*};
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use log::info;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{mpsc::Sender, Arc};

#[actix_rt::main]
pub async fn start(tx: Sender<String>, conf: Arc<config::Config>) -> std::io::Result<()> {
    info!(
        "router is listening on http://0.0.0.0:{}",
        conf.router.http_port
    );

    let arc_service = Arc::new(
        RouterService::new(conf.clone())
            .await
            .expect(format!("router failed to connect the master ",).as_str()),
    );

    HttpServer::new(move || {
        App::new()
            .data(arc_service.clone())
            .route("/", web::get().to(domain))
            .route("/get/{collection_name}/{id}", web::get().to(get))
            .route("/put/{collection_name}/{id}", web::post().to(put))
            .route("/update/{collection_name}/{id}", web::post().to(update))
            .route("/upsert/{collection_name}/{id}", web::post().to(upsert))
            .route("/create/{collection_name}/{id}", web::post().to(create))
            .route("/delete/{collection_name}/{id}", web::delete().to(delete))
            .route("/search/{collection_names}", web::get().to(search))
            .route("/count/{collection_name}", web::get().to(count))
            //command
            .route("/command", web::post().to(command))
    })
    .bind(format!("0.0.0.0:{}", conf.router.http_port))?
    .run()
    .await
    .unwrap();

    let _ = tx.send(String::from("router has been over"));

    Ok(())
}

async fn domain() -> HttpResponse {
    HttpResponse::build(http_code(SUCCESS)).body(json!({
        "chubaodb":"router is runing",
        "version":config::VERSION,
        "git_version": config::GIT_VERSION,
    }))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DocumentQuery {
    pub version: Option<i64>,
    pub sort_key: Option<String>,
}

//this api was deprecated
//example : {"target":["127.0.0.1:9090"] , "method":"file_info" , "path":"./"}
async fn command(rs: web::Data<Arc<RouterService>>, bytes: web::Bytes) -> HttpResponse {
    match rs.command(bytes.to_vec()).await {
        Ok(s) => HttpResponse::build(http_code(SUCCESS)).json(s),
        Err(e) => {
            let e = cast_to_err(e);
            HttpResponse::build(http_code(e.0))
                .content_type("application/json")
                .body(e.to_json())
        }
    }
}

async fn write(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    bytes: Option<web::Bytes>,
    query: DocumentQuery,
    wt: i32,
) -> HttpResponse {
    let collection_name: String = req
        .match_info()
        .get("collection_name")
        .unwrap()
        .parse()
        .unwrap();
    let id: String = req.match_info().get("id").unwrap().parse().unwrap();

    let bytes = match bytes {
        Some(v) => v.to_vec(),
        None => Vec::default(),
    };

    match rs
        .write(
            collection_name,
            id,
            query.sort_key.unwrap_or(String::default()),
            query.version.unwrap_or(0),
            bytes,
            wt,
        )
        .await
    {
        Ok(s) => HttpResponse::build(http_code(SUCCESS)).json(gr_to_json(s)),
        Err(e) => {
            let e = cast_to_err(e);
            HttpResponse::build(http_code(e.0))
                .content_type("application/json")
                .body(e.to_json())
        }
    }
}

async fn create(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    query: web::Query<DocumentQuery>,
    bytes: web::Bytes,
) -> HttpResponse {
    write(
        rs,
        req,
        Some(bytes),
        query.into_inner(),
        WriteType::Create as i32,
    )
    .await
}

async fn put(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    query: web::Query<DocumentQuery>,
    bytes: web::Bytes,
) -> HttpResponse {
    write(
        rs,
        req,
        Some(bytes),
        query.into_inner(),
        WriteType::Put as i32,
    )
    .await
}

async fn update(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    query: web::Query<DocumentQuery>,
    bytes: web::Bytes,
) -> HttpResponse {
    write(
        rs,
        req,
        Some(bytes),
        query.into_inner(),
        WriteType::Update as i32,
    )
    .await
}

async fn upsert(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    query: web::Query<DocumentQuery>,
    bytes: web::Bytes,
) -> HttpResponse {
    write(
        rs,
        req,
        Some(bytes),
        query.into_inner(),
        WriteType::Upsert as i32,
    )
    .await
}

async fn delete(
    rs: web::Data<Arc<RouterService>>,
    query: web::Query<DocumentQuery>,
    req: HttpRequest,
) -> HttpResponse {
    write(rs, req, None, query.into_inner(), WriteType::Delete as i32).await
}

async fn get(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    query: web::Query<DocumentQuery>,
) -> HttpResponse {
    let collection_name: String = req
        .match_info()
        .get("collection_name")
        .unwrap()
        .parse()
        .unwrap();
    let id: String = req.match_info().get("id").unwrap().parse().unwrap();

    match rs
        .get(
            collection_name,
            id,
            query.into_inner().sort_key.unwrap_or(String::default()),
        )
        .await
    {
        Ok(s) => HttpResponse::build(http_code(SUCCESS)).json(doc_to_json(s)),
        Err(e) => {
            let e = cast_to_err(e);
            HttpResponse::build(http_code(e.0))
                .content_type("application/json")
                .body(e.to_json())
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Query {
    pub def_fields: Option<String>,
    pub query: Option<String>,
    pub size: Option<u32>,
}

async fn count(rs: web::Data<Arc<RouterService>>, req: HttpRequest) -> HttpResponse {
    let collection_name: String = req
        .match_info()
        .get("collection_name")
        .unwrap()
        .parse()
        .unwrap();

    match rs.count(collection_name).await {
        Ok(s) => HttpResponse::build(http_code(SUCCESS)).json(json!({
            "code":s.code,
            "estimate_count":s.estimate_count,
            "index_count":s.index_count,
            "message":s.message
        })),
        Err(e) => {
            let e = cast_to_err(e);
            HttpResponse::build(http_code(e.0))
                .content_type("application/json")
                .body(e.to_json())
        }
    }
}

async fn search(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    query: web::Query<Query>,
) -> HttpResponse {
    let mut collection_names = Vec::new();

    for n in req
        .match_info()
        .get("collection_names")
        .unwrap()
        .parse::<String>()
        .unwrap()
        .split(",")
    {
        collection_names.push(n.to_string());
    }

    let query = query.into_inner();

    let mut def_fields = Vec::new();

    match query.def_fields {
        Some(dfs) => {
            for df in dfs.split(",") {
                def_fields.push(df.to_string());
            }
        }
        None => {}
    };

    match rs
        .search(
            collection_names,
            def_fields,
            query.query.unwrap_or(String::from("*")),
            query.size.unwrap_or(20),
        )
        .await
    {
        Ok(s) => HttpResponse::build(http_code(SUCCESS)).json(search_to_json(s)),
        Err(e) => {
            let e = cast_to_err(e);
            return HttpResponse::build(http_code(e.0))
                .content_type("application/json")
                .body(e.to_json());
        }
    }
}

fn search_to_json(sdr: SearchDocumentResponse) -> serde_json::value::Value {
    let (success, error, message) = match sdr.info {
        Some(i) => (i.success, i.error, i.message),
        None => (1, 0, String::default()),
    };

    let mut hits = Vec::new();
    for hit in sdr.hits {
        let doc: Document = match Message::decode(prost::bytes::Bytes::from(hit.doc)) {
            Ok(d) => d,
            Err(e) => {
                return json!({
                    "code": INTERNAL_ERR ,
                    "info": {
                        "message":format!("document decoding failed:{}", e.to_string())
                    },
                });
            }
        };

        let source: Value = match serde_json::from_slice(doc.source.as_slice()) {
            Ok(v) => v,
            Err(e) => {
                return json!({
                    "code": INTERNAL_ERR ,
                    "message": format!("source decoding failed:{}", e.to_string()),
                });
            }
        };

        hits.push(json!({
            "score": hit.score ,
            "doc":{
                "_id": doc.id,
                "_sort_key": doc.sort_key,
                "_version": doc.version,
                "_source":source,
            },
        }));
    }

    return json!({
        "code": sdr.code ,
        "total": sdr.total ,
        "hits":hits,
        "info":{
            "success": success ,
            "error": error ,
            "message": message ,
        }

    });
}

fn doc_to_json(dr: DocumentResponse) -> serde_json::value::Value {
    if dr.doc.len() == 0 {
        return json!({
            "code": dr.code ,
            "message": dr.message,
        });
    }

    let doc: Document = match Message::decode(prost::bytes::Bytes::from(dr.doc)) {
        Ok(d) => d,
        Err(e) => {
            return json!({
                "code": INTERNAL_ERR ,
                "message": format!("document decoding failed:{}", e.to_string()),
            });
        }
    };

    let source: Value = match serde_json::from_slice(doc.source.as_slice()) {
        Ok(v) => v,
        Err(e) => {
            return json!({
                "code": INTERNAL_ERR ,
                "message": format!("source decoding failed:{}", e.to_string()),
            });
        }
    };

    json!({
        "code": dr.code ,
        "message": dr.message,
        "doc":{
            "_id": doc.id,
            "_sort_key": doc.sort_key,
            "_version": doc.version,
            "_source": source,
        },
    })
}

fn gr_to_json(gr: GeneralResponse) -> serde_json::value::Value {
    json!({
        "code": gr.code ,
        "message": gr.message,
    })
}

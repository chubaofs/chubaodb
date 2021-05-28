use std::sync::{mpsc::Sender, Arc};

use crate::router::service::RouterService;
use crate::util::{config, error::*};
use crate::*;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use alaya_protocol::pserver::*;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::log::{error, info};

#[actix_web::main]
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
            .route("/search/{collection_names}", web::get().to(search_by_get))
            .route("/search/{collection_names}", web::post().to(search_by_post))
            .route("/agg/{collection_names}", web::get().to(agg_by_get))
            .route("/agg/{collection_names}", web::post().to(agg_by_post))
            .route("/count/{collection_name}", web::get().to(count))
    })
    .bind(format!("0.0.0.0:{}", conf.router.http_port))?
    .run()
    .await
    .unwrap();

    let _ = tx.send(String::from("router has been over"));

    Ok(())
}

async fn domain() -> HttpResponse {
    HttpResponse::build(Code::Success.http_code()).body(json!({
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
        Ok(s) => HttpResponse::build(Code::Success.http_code()).json(gr_to_json(s)),
        Err(e) => HttpResponse::build(e.code().http_code())
            .content_type("application/json")
            .body(e.to_json()),
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
        Ok(s) => HttpResponse::build(Code::Success.http_code()).json(doc_to_json(s)),
        Err(e) => HttpResponse::build(e.code().http_code())
            .content_type("application/json")
            .body(e.to_json()),
    }
}

async fn count(rs: web::Data<Arc<RouterService>>, req: HttpRequest) -> HttpResponse {
    let collection_name: String = req
        .match_info()
        .get("collection_name")
        .unwrap()
        .parse()
        .unwrap();

    match rs.count(collection_name).await {
        Ok(s) => {
            HttpResponse::build(Code::Success.http_code()).json(serde_json::to_value(&s).unwrap())
        }
        Err(e) => HttpResponse::build(e.code().http_code())
            .content_type("application/json")
            .body(e.to_json()),
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TempVectorQuery {
    pub field: Option<String>,
    pub vector: Vec<f32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Query {
    pub query: Option<String>,
    pub def_fields: Option<String>,
    pub vector_query: Option<TempVectorQuery>,
    pub size: Option<u32>,
    pub sort: Option<String>, //name:asc|age:desc
    pub fun: Option<String>,
    pub group: Option<String>,
}

// search begin
async fn search_by_post(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    info: web::Bytes,
) -> HttpResponse {
    let names = req
        .match_info()
        .get("collection_names")
        .unwrap()
        .parse::<String>()
        .unwrap();

    let query: Query = match serde_json::from_slice(&info) {
        Ok(v) => v,
        Err(e) => {
            error!("query parse has err:{:?}", e);
            return HttpResponse::build(Code::ParamError.http_code())
                .body(err_def!("query has err:{:?}", e).to_json());
        }
    };

    match _search(rs, names, query).await {
        Ok(s) => HttpResponse::build(Code::Success.http_code()).json(search_to_json(s)),
        Err(e) => HttpResponse::build(e.code().http_code())
            .content_type("application/json")
            .body(e.to_json()),
    }
}

async fn search_by_get(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    query: web::Query<Query>,
) -> HttpResponse {
    let names = req
        .match_info()
        .get("collection_names")
        .unwrap()
        .parse::<String>()
        .unwrap();

    let query = query.into_inner();

    match _search(rs, names, query).await {
        Ok(s) => HttpResponse::build(Code::Success.http_code()).json(search_to_json(s)),
        Err(e) => HttpResponse::build(e.code().http_code())
            .content_type("application/json")
            .body(e.to_json()),
    }
}

async fn _search(
    rs: web::Data<Arc<RouterService>>,
    names: String,
    query: Query,
) -> ASResult<SearchDocumentResponse> {
    let mut collection_names = Vec::new();

    for n in names.split(",") {
        let name = n.to_string();
        if name.len() == 0 {
            continue;
        }
        collection_names.push(name);
    }

    let sort = parse_sort(&query)?;

    let mut def_fields = Vec::new();

    match query.def_fields {
        Some(dfs) => {
            for df in dfs.split(",") {
                def_fields.push(df.to_string());
            }
        }
        None => {}
    };

    let vq = match query.vector_query {
        Some(tvq) => Some(VectorQuery {
            field: match tvq.field {
                Some(field) => field,
                None => {
                    return result!(Code::ParamError, "vector query not set field");
                }
            },
            vector: tvq.vector,
        }),
        None => None,
    };

    rs.search(
        collection_names,
        def_fields,
        query.query.unwrap_or(String::from("*")),
        vq,
        query.size.unwrap_or(20),
        sort,
    )
    .await
}

// agg begin
async fn agg_by_post(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    info: web::Bytes,
) -> HttpResponse {
    let names = req
        .match_info()
        .get("collection_names")
        .unwrap()
        .parse::<String>()
        .unwrap();

    let query: Query = match serde_json::from_slice(&info) {
        Ok(v) => v,
        Err(e) => {
            error!("query parse has err:{:?}", e);
            return HttpResponse::build(Code::ParamError.http_code())
                .body(err_def!("query has err:{:?}", e).to_json());
        }
    };

    match _agg(rs, names, query).await {
        Ok(s) => HttpResponse::build(Code::Success.http_code()).json(agg_to_json(s)),
        Err(e) => HttpResponse::build(e.code().http_code())
            .content_type("application/json")
            .body(e.to_json()),
    }
}

async fn agg_by_get(
    rs: web::Data<Arc<RouterService>>,
    req: HttpRequest,
    query: web::Query<Query>,
) -> HttpResponse {
    let names = req
        .match_info()
        .get("collection_names")
        .unwrap()
        .parse::<String>()
        .unwrap();

    let query = query.into_inner();

    match _agg(rs, names, query).await {
        Ok(s) => HttpResponse::build(Code::Success.http_code()).json(agg_to_json(s)),
        Err(e) => HttpResponse::build(e.code().http_code())
            .content_type("application/json")
            .body(e.to_json()),
    }
}

async fn _agg(
    rs: web::Data<Arc<RouterService>>,
    names: String,
    query: Query,
) -> ASResult<AggregationResponse> {
    let mut collection_names = Vec::new();

    for n in names.split(",") {
        let name = n.to_string();
        if name.len() == 0 {
            continue;
        }
        collection_names.push(name);
    }

    let sort = parse_sort(&query)?;

    let mut def_fields = Vec::new();

    match query.def_fields {
        Some(dfs) => {
            for df in dfs.split(",") {
                def_fields.push(df.to_string());
            }
        }
        None => {}
    };

    let vq = match query.vector_query {
        Some(tvq) => Some(VectorQuery {
            field: match tvq.field {
                Some(field) => field,
                None => {
                    return result!(Code::ParamError, "vector query not set field");
                }
            },
            vector: tvq.vector,
        }),
        None => None,
    };

    rs.agg(
        collection_names,
        def_fields,
        query.query.unwrap_or(String::from("*")),
        vq,
        query.size.unwrap_or(10000),
        query.group.unwrap_or(String::from("")),
        query.fun.unwrap_or(String::from("")),
        sort,
    )
    .await
}

fn agg_to_json(adr: AggregationResponse) -> serde_json::value::Value {
    let (success, error, message) = match adr.info {
        Some(i) => (i.success, i.error, i.message),
        None => (1, 0, String::default()),
    };

    let mut result = vec![];

    for v in adr.result {
        let jv = v
            .values
            .into_iter()
            .map(|r| match r.agg_value.unwrap() {
                agg_value::AggValue::Count(r) => json!(r),
                agg_value::AggValue::Stats(r) => json!(r),
                agg_value::AggValue::Hits(r) => json!({
                    "size":r.size,
                    "count":r.count,
                    "hits": r.hits.into_iter().map(|h|hit_to_json(h).unwrap()).collect::<Vec<Value>>(),
                }),
            })
            .collect::<Vec<Value>>();

        result.push(json!({
            "key": v.key,
            "values":jv,
        }));
    }

    return json!({
        "code": adr.code ,
        "total": adr.total ,
        "result":result,
        "info":{
            "success": success ,
            "error": error ,
            "message": message ,
        }

    });
}

fn search_to_json(sdr: SearchDocumentResponse) -> serde_json::value::Value {
    let (success, error, message) = match sdr.info {
        Some(i) => (i.success, i.error, i.message),
        None => (1, 0, String::default()),
    };

    let mut hits = Vec::new();
    for hit in sdr.hits {
        match hit_to_json(hit) {
            Ok(v) => hits.push(v),
            Err(e) => {
                return json!({
                    "code": Code::InternalErr as i32 ,
                    "info": {
                        "message":format!("document decoding failed:{}", e.to_string())
                    },
                })
            }
        }
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

fn hit_to_json(hit: Hit) -> ASResult<serde_json::value::Value> {
    let doc: Document = match Message::decode(prost::bytes::Bytes::from(hit.doc)) {
        Ok(d) => d,
        Err(e) => {
            return result!(
                Code::InternalErr,
                "document decoding failed:{}",
                e.to_string()
            );
        }
    };

    let source: Value = match serde_json::from_slice(doc.source.as_slice()) {
        Ok(v) => v,
        Err(e) => {
            return result!(
                Code::InternalErr,
                "source decoding failed:{}",
                e.to_string()
            );
        }
    };

    Ok(json!({
        "score": hit.score ,
        "doc":{
            "_id": doc.id,
            "_sort_key": doc.sort_key,
            "_version": doc.version,
            "_source":source,
        },
    }))
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
                "code": Code::InternalErr as i32 ,
                "message": format!("document decoding failed:{}", e.to_string()),
            });
        }
    };

    let source: Value = match serde_json::from_slice(doc.source.as_slice()) {
        Ok(v) => v,
        Err(e) => {
            return json!({
                "code": Code::InternalErr as i32 ,
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

fn parse_sort(query: &Query) -> ASResult<Vec<Order>> {
    if let Some(sort) = query.sort.as_ref() {
        sort.split(",")
            .map(|s| s.split(":").collect::<Vec<&str>>())
            .map(|s| {
                if s.len() != 2 {
                    return result!(
                        Code::ParamError,
                        "sort param:[{:?}] has format has err, example:[name:asc|age:desc]",
                        s
                    );
                }

                let name = s[0].to_owned();
                let order = s[1].to_lowercase();

                match order.as_str() {
                    "asc" | "desc" => {}
                    _ => {
                        return result!(
                            Code::ParamError,
                            "sort param name:{} order:{} only support asc or desc",
                            name,
                            order
                        )
                    }
                }
                Ok(Order {
                    name: name,
                    order: order,
                })
            })
            .collect()
    } else {
        Ok(vec![])
    }
}

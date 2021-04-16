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
use crate::master::graphql::{MasterSchema, Mutation, Query};
use crate::master::service::MasterService;
use crate::util::{config, error::*};
use actix_web::{guard, web, App, HttpRequest, HttpResponse, HttpServer};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql::{EmptySubscription, Schema};
use async_graphql_actix_web::{Request, Response};
use tracing::log::info;
use serde_json::json;

use std::sync::{mpsc::Sender, Arc};

#[actix_rt::main]
pub async fn start(tx: Sender<String>, conf: Arc<config::Config>) -> std::io::Result<()> {
	let http_port = match conf.self_master() {
		Some(m) => m.http_port,
		None => panic!("self not set in config "),
	};

	let service = Arc::new(
		MasterService::new(conf.clone())
			.await
			.expect(format!("master service init err").as_str()),
	);

	let schema = Schema::build(Query, Mutation, EmptySubscription)
		.data(service.clone())
		.finish();

	info!("master listening on http://0.0.0.0:{}", http_port);

	HttpServer::new(move || {
		App::new()
			.data(service.clone())
			.data(schema.clone())
			// .service(web::resource("/").guard(guard::Post()).to(graphql))
			.service(web::resource("/").guard(guard::Get()).to(graphiql))
			//admin handler
			.route("/my_ip", web::get().to(my_ip))
	})
		.bind(format!("0.0.0.0:{}", http_port))?
		.run()
		.await
		.unwrap();

	let _ = tx.send(String::from("master has over"));

	Ok(())
}

async fn graphql(
	schema: web::Data<MasterSchema>,
	req: Request,
) -> Response {
	schema.execute(req.into_inner()).await.into()
}

async fn graphiql() -> HttpResponse {
	HttpResponse::Ok()
		.content_type("text/html; charset=utf-8")
		.body(playground_source(GraphQLPlaygroundConfig::new("/")))
}

async fn my_ip(req: HttpRequest) -> HttpResponse {
	let remote = req
		.connection_info()
		.remote()
		.to_owned()
		.unwrap()
		.to_string();
	let addr: Vec<&str> = remote.split(":").collect();

	HttpResponse::build(Code::Success.http_code()).json(json!({
        "ip":addr[0],
    }))
}

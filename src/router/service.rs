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
use crate::client::ps_client::PsClient;
use crate::util::{config::Config, error::*};
use alaya_protocol::pserver::*;
use std::sync::Arc;

pub struct RouterService {
    ps_client: Arc<PsClient>,
}

impl RouterService {
    pub async fn new(conf: Arc<Config>) -> ASResult<RouterService> {
        Ok(RouterService {
            ps_client: Arc::new(PsClient::new(conf)),
        })
    }

    pub async fn write(
        &self,
        collection_name: String,
        id: String,
        sort_key: String,
        version: i64,
        source: Vec<u8>,
        wt: i32,
    ) -> ASResult<GeneralResponse> {
        self.ps_client
            .write(collection_name, id, sort_key, version, source, wt)
            .await
    }

    pub async fn get(
        &self,
        collection_name: String,
        id: String,
        sort_key: String,
    ) -> ASResult<DocumentResponse> {
        self.ps_client.get(collection_name, id, sort_key).await
    }

    pub async fn search(
        &self,
        collection_names: Vec<String>,
        def_fields: Vec<String>,
        query: String,
        vector_query: Option<VectorQuery>,
        size: u32,
        sort: Vec<Order>,
    ) -> ASResult<SearchDocumentResponse> {
        self.ps_client
            .multiple_search(
                collection_names,
                QueryRequest {
                    cpids: vec![],
                    query: query,
                    def_fields: def_fields,
                    vector_query: vector_query,
                    size: size,
                    sort: sort,
                    fun: Default::default(),
                    group: Default::default(),
                },
            )
            .await
    }

    pub async fn agg(
        &self,
        collection_names: Vec<String>,
        def_fields: Vec<String>,
        query: String,
        vector_query: Option<VectorQuery>,
        size: u32,
        group: String,
        fun: String,
        sort: Vec<Order>,
    ) -> ASResult<AggregationResponse> {
        self.ps_client
            .agg(
                collection_names[0].as_str(),
                QueryRequest {
                    cpids: vec![],
                    query,
                    def_fields,
                    vector_query,
                    size,
                    group,
                    fun,
                    sort,
                },
            )
            .await
    }

    pub async fn count(&self, collection_name: String) -> ASResult<CountDocumentResponse> {
        self.ps_client.count(collection_name.as_str()).await
    }
}

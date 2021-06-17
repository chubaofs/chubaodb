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
use crate::util::{config::Config, error::*};
use alaya_protocol::pserver::*;
use std::sync::Arc;

pub struct RouterService {}

impl RouterService {
    pub async fn new(conf: Arc<Config>) -> ASResult<RouterService> {
        Ok(RouterService {})
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
        panic!();
    }

    pub async fn get(
        &self,
        collection_name: String,
        id: String,
        sort_key: String,
    ) -> ASResult<DocumentResponse> {
        panic!();
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
        panic!();
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
        panic!();
    }

    pub async fn count(&self, collection_name: String) -> ASResult<CountDocumentResponse> {
        panic!();
    }
}

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
use crate::util::{config::*, entity::*, error::*, http_client};
use crate::*;
use serde_json::json;
use std::str;
use std::sync::Arc;

const DEF_TIME_OUT: u64 = 30000;

pub struct MetaClient {
    conf: Arc<Config>,
}

impl MetaClient {
    pub fn new(conf: Arc<Config>) -> Self {
        MetaClient { conf }
    }

    pub async fn my_ip(&self) -> ASResult<String> {
        let url = format!("http://{}/my_ip", self.conf.master_addr());
        let value: serde_json::Value = http_client::get_json(&url, DEF_TIME_OUT).await?;

        match value.get("ip") {
            Some(ip) => Ok(ip.as_str().unwrap().to_string()),
            None => result_def!("got ip from master:{} is no ip", url),
        }
    }

    pub async fn pserver_update(&self, pserver: &PServer) -> ASResult<()> {
        let query = r#"
            mutation($json: JSON!){
                pserverUpdate(json : $json) 
            }
        "#;

        let _: serde_json::Value = http_client::graphql(
            &self.conf.master_http_addr(),
            DEF_TIME_OUT,
            query,
            json!({
                "json": pserver,
            }),
            "pserverUpdate",
        )
        .await?;

        Ok(())
    }

    pub async fn pserver_get(&self, id: u64) -> ASResult<PServer> {
        let query = r#"
            query($id: Int!){
                pserverGet(id : $id) 
            }
        "#;

        http_client::graphql(
            &self.conf.master_http_addr(),
            DEF_TIME_OUT,
            query,
            json!({
                "id": id,
            }),
            "pserverGet",
        )
        .await
    }

    pub async fn register(
        &self,
        ip: &str,
        port: u16,
        raft_heart_port: u16,
        raft_log_port: u16,
    ) -> ASResult<PServer> {
        let pserver = PServer::new(
            self.conf.ps.zone.clone(),
            None,
            ip,
            port,
            raft_heart_port,
            raft_log_port,
        );

        let query = r#"
            mutation($json: JSON!){
                pserverRegister(json : $json) 
            }
        "#;

        http_client::graphql(
            &self.conf.master_http_addr(),
            DEF_TIME_OUT,
            query,
            json!({
                "json": pserver,
            }),
            "pserverRegister",
        )
        .await
    }

    pub async fn partition_get(
        &self,
        collection_id: u32,
        partition_id: u32,
    ) -> ASResult<Partition> {
        let query = r#"
            query($collectionId: Int!, $partitionId: Int!){
                partitionGet(collectionId : $collectionId, partitionId:$partitionId) 
            }
        "#;

        http_client::graphql(
            &self.conf.master_http_addr(),
            DEF_TIME_OUT,
            query,
            json!({
                "collectionId": collection_id,
                "partitionId": partition_id,
            }),
            "partitionGet",
        )
        .await
    }

    pub async fn partition_update(&self, partition: &Partition) -> ASResult<()> {
        let query = r#"
            mutation($json: JSON!){
                partitionUpdate(json : $json) 
            }
        "#;

        let _: serde_json::Value = http_client::graphql(
            &self.conf.master_http_addr(),
            DEF_TIME_OUT,
            query,
            json!({
                "json": partition,
            }),
            "partitionUpdate",
        )
        .await?;
        Ok(())
    }

    pub async fn collection_get(
        &self,
        id: Option<u32>,
        name: Option<&str>,
    ) -> ASResult<Collection> {
        let query = r#"
            query($id: Int, $name: String){
                collectionGet(id : $id, name:$name) 
            }
        "#;

        let mut collection: Collection = http_client::graphql(
            &self.conf.master_http_addr(),
            DEF_TIME_OUT,
            query,
            json!({
                "id": id,
                "name": name,
            }),
            "collectionGet",
        )
        .await?;

        collection.init();
        Ok(collection)
    }
}

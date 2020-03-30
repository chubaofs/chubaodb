// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.
use crate::util::{config::*, entity::*, error::*, http_client};
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
            None => Err(err_box(format!("got ip from master:{} is no ip", url))),
        }
    }

    pub async fn put_pserver(&self, pserver: &PServer) -> ASResult<()> {
        let url = format!("http://{}/pserver/put", self.conf.master_addr());
        let _: PServer = http_client::post_json(&url, DEF_TIME_OUT, pserver).await?;
        Ok(())
    }

    pub async fn heartbeat(
        &self,
        zone_id: u32,
        ps_id: Option<u64>,
        ip: &str,
        port: u32,
    ) -> ASResult<PServer> {
        let url = format!("http://{}/pserver/heartbeat", self.conf.master_addr());
        let pserver = PServer::new(zone_id, None, format!("{}:{}", ip, port));
        http_client::post_json(&url, DEF_TIME_OUT, &pserver).await
    }

    pub async fn get_partition(
        &self,
        collection_id: u32,
        partition_id: u32,
    ) -> ASResult<Partition> {
        let url = format!(
            "http://{}/partition/get/{}/{}",
            self.conf.master_addr(),
            collection_id,
            partition_id
        );

        http_client::get_json(&url, DEF_TIME_OUT).await
    }

    pub async fn update_partition(&self, partition: &Partition) -> ASResult<()> {
        let url = format!(
            "http://{}/collection/partition/update",
            self.conf.master_addr(),
        );

        http_client::post_json(&url, DEF_TIME_OUT, partition).await
    }

    pub async fn get_collection(&self, name: &str) -> ASResult<Collection> {
        let url = format!("http://{}/collection/get/{}", self.conf.master_addr(), name);

        http_client::get_json(&url, DEF_TIME_OUT).await
    }

    pub async fn get_collection_by_id(&self, collection_id: u32) -> ASResult<Collection> {
        let url = format!(
            "http://{}/collection/get_by_id/{}",
            self.conf.master_addr(),
            collection_id,
        );

        http_client::get_json(&url, DEF_TIME_OUT).await
    }
}

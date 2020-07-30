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
use crate::client::meta_client::MetaClient;
use crate::client::partition_client::*;
use crate::pserver::simba::aggregation;
use crate::pserverpb::rpc_client::RpcClient;
use crate::pserverpb::*;
use crate::util::{coding, config, entity::*, error::*};
use crate::*;
use async_std::{sync::channel, task};
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tonic::transport::{Channel, Endpoint};

const RETRY: usize = 5;

pub struct CollectionInfo {
    pub collection: Collection,
    pub partitions: Vec<Partition>,
    pub fields: HashMap<String, Field>,
}

pub struct PsClient {
    _conf: Arc<config::Config>,
    meta_cli: MetaClient,
    lock_cache: RwLock<HashMap<String, Arc<Mutex<usize>>>>,
    collection_cache: RwLock<HashMap<String, Arc<CollectionInfo>>>,
    channel_cache: RwLock<HashMap<String, RpcClient<Channel>>>,
}

impl PsClient {
    pub fn new(conf: Arc<config::Config>) -> Self {
        PsClient {
            _conf: conf.clone(),
            lock_cache: RwLock::new(HashMap::new()),
            meta_cli: MetaClient::new(conf.clone()),
            collection_cache: RwLock::new(HashMap::new()),
            channel_cache: RwLock::new(HashMap::new()),
        }
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
        'outer: for i in 0..RETRY {
            match self
                ._write(
                    collection_name.as_str(),
                    id.as_str(),
                    sort_key.as_str(),
                    version,
                    &source,
                    wt,
                )
                .await
            {
                Ok(r) => {
                    return Ok(r);
                }
                Err(e) => {
                    if self.check_err_cache(i, collection_name.as_str(), &e) {
                        error!("write document has err:[{:?}] , it will be retry!", e);
                        continue 'outer;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        panic!("out of range")
    }

    async fn _write(
        &self,
        collection_name: &str,
        id: &str,
        sort_key: &str,
        version: i64,
        source: &Vec<u8>,
        wt: i32,
    ) -> ASResult<GeneralResponse> {
        let ps = self.select_partition(collection_name, id).await?;

        ps.write(
            RpcClient::new(Endpoint::from_shared(ps.addr())?.connect().await?),
            WriteDocumentRequest {
                collection_id: ps.collection_id,
                partition_id: ps.partition_id,
                doc: Some(Document {
                    id: id.to_string(),
                    sort_key: sort_key.to_string(),
                    source: source.to_owned(),
                    slot: ps.slot,
                    partition_id: ps.partition_id,
                    version: version,
                    vectors: Vec::default(),
                    scalars: Vec::default(),
                }),
                write_type: wt,
            },
        )
        .await
    }

    pub async fn get(
        &self,
        collection_name: String,
        id: String,
        sort_key: String,
    ) -> ASResult<DocumentResponse> {
        'outer: for i in 0..RETRY {
            match self
                ._get(collection_name.as_str(), id.as_str(), sort_key.as_str())
                .await
            {
                Ok(r) => {
                    return Ok(r);
                }
                Err(e) => {
                    if self.check_err_cache(i, collection_name.as_str(), &e) {
                        error!("get document has err:[{:?}] , will may be retry!", e);
                        continue 'outer;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        panic!("out of range")
    }
    pub async fn _get(
        &self,
        collection_name: &str,
        id: &str,
        sort_key: &str,
    ) -> ASResult<DocumentResponse> {
        let ps = self.select_partition(collection_name, id).await?;

        ps.get(
            self.channel_cache(ps.addr.as_str()).await?,
            GetDocumentRequest {
                collection_id: ps.collection_id,
                partition_id: ps.partition_id,
                id: id.to_string(),
                sort_key: sort_key.to_string(),
            },
        )
        .await
    }

    pub async fn multiple_search(
        self: &Arc<Self>,
        collection_name: Vec<String>,
        query: QueryRequest,
    ) -> ASResult<SearchDocumentResponse> {
        let (tx, rx) = channel(10);
        for name in collection_name {
            let tx = tx.clone();
            let cli = self.clone();
            let query = query.clone();
            task::spawn(async move { tx.send(cli.search(name.as_str(), query).await).await });
        }

        drop(tx);

        let mut dist = rx.recv().await.unwrap()?;

        while let Ok(src) = rx.recv().await {
            let src = src?;
            dist = merge_search_document_response(dist, src);
        }

        return Ok(dist);
    }

    pub async fn search(
        &self,
        collection_name: &str,
        query: QueryRequest,
    ) -> ASResult<SearchDocumentResponse> {
        'outer: for i in 0..RETRY {
            let (tx, rx) = channel(10);

            match self.select_collection(collection_name).await {
                Ok(mpl) => {
                    for mp in mpl {
                        let mut query = query.clone();
                        query.cpids = mp.collection_partition_ids.clone();
                        let tx = tx.clone();
                        task::spawn(async move {
                            match mp.search(query).await {
                                Ok(resp) => {
                                    tx.send(resp).await;
                                }
                                Err(e) => {
                                    tx.send(e.into()).await;
                                }
                            };
                        });
                    }
                }
                Err(e) => {
                    if self.check_err_cache(i, collection_name, &e) {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };

            drop(tx);

            let mut dist = rx.recv().await.unwrap();

            if Code::from_i32(dist.code) != Code::Success
                && self.check_response_cache(
                    i,
                    collection_name,
                    err!(dist.code, msg_for_resp(&dist.info)),
                )
            {
                continue 'outer;
            }
            while let Ok(src) = rx.recv().await {
                if Code::from_i32(src.code) != Code::Success
                    && self.check_response_cache(
                        i,
                        collection_name,
                        err!(dist.code, msg_for_resp(&dist.info)),
                    )
                {
                    continue 'outer;
                }
                dist = merge_search_document_response(dist, src);
            }

            return Ok(dist);
        }
        panic!("out of range");
    }

    pub async fn agg(
        &self,
        collection_name: &str,
        query: QueryRequest,
    ) -> ASResult<AggregationResponse> {
        'outer: for i in 0..RETRY {
            let (tx, rx) = channel::<AggregationResponse>(10);

            let mut result_size: i32 = 0;

            match self.select_collection(collection_name).await {
                Ok(mpl) => {
                    for mp in mpl {
                        let mut query = query.clone();
                        query.cpids = mp.collection_partition_ids.clone();
                        let tx = tx.clone();
                        result_size += 1;
                        task::spawn(async move {
                            match mp.agg(query).await {
                                Ok(resp) => tx.send(resp).await,
                                Err(e) => tx.send(e.into()).await,
                            };
                        });
                    }
                }
                Err(e) => {
                    if self.check_err_cache(i, collection_name, &e) {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };

            drop(tx);

            let mut dist = rx.recv().await.unwrap();

            if result_size == 1 {
                return Ok(dist);
            }

            let mut result = HashMap::new();
            for v in std::mem::replace(&mut dist.result, Vec::default()) {
                result.insert(v.key.clone(), v);
            }

            if Code::from_i32(dist.code) != Code::Success
                && self.check_response_cache(
                    i,
                    collection_name,
                    err!(dist.code, msg_for_resp(&dist.info)),
                )
            {
                continue 'outer;
            }
            while let Ok(src) = rx.recv().await {
                if Code::from_i32(src.code) != Code::Success
                    && self.check_response_cache(
                        i,
                        collection_name,
                        err!(dist.code, msg_for_resp(&dist.info)),
                    )
                {
                    continue 'outer;
                }
                dist = merge_aggregation_response(dist, &mut result, src);
            }

            dist.result = aggregation::make_vec(result, &query.sort, query.size as usize)?;

            return Ok(dist);
        }
        panic!("out of range");
    }

    pub async fn count(&self, collection_name: &str) -> ASResult<CountDocumentResponse> {
        'outer: for i in 0..RETRY {
            let (tx, rx) = channel::<CountDocumentResponse>(10);

            match self.select_collection(collection_name).await {
                Ok(mpl) => {
                    for mp in mpl {
                        let tx = tx.clone();
                        task::spawn(async move {
                            match mp.count().await {
                                Ok(resp) => tx.send(resp).await,
                                Err(e) => {
                                    if let Err(e) = tx.try_send(e.into()) {
                                        error!("send result has err:{:?}", e); //TODO: if errr
                                    };
                                }
                            };
                        });
                    }
                }
                Err(e) => {
                    if self.check_err_cache(i, collection_name, &e) {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };

            drop(tx);

            let mut dist = rx.recv().await.unwrap();

            if Code::from_i32(dist.code) != Code::Success
                && self.check_response_cache(i, collection_name, err!(dist.code, dist.message))
            {
                continue 'outer;
            }

            while let Ok(src) = rx.recv().await {
                if Code::from_i32(src.code) != Code::Success
                    && self.check_response_cache(i, collection_name, err!(dist.code, dist.message))
                {
                    continue 'outer;
                }
                dist = merge_count_document_response(dist, src);
            }

            return Ok(dist);
        }
        panic!("out of range");
    }

    pub async fn status(&self, addr: &str) -> ASResult<GeneralResponse> {
        let result = PartitionClient::new(addr.to_string())
            .status(GeneralRequest {
                collection_id: 0,
                partition_id: 0,
            })
            .await?;

        if Code::from_i32(result.code) != Code::Success {
            return result!(result.code, result.message);
        }

        Ok(result)
    }

    async fn select_collection(&self, name: &str) -> ASResult<Vec<MultiplePartitionClient>> {
        let c: Arc<CollectionInfo> = self.cache_collection(name).await?;

        let mut map = HashMap::new();

        for partition in c.partitions.iter() {
            let mp = map
                .entry(partition.leader.clone())
                .or_insert(MultiplePartitionClient::new(partition.leader.clone()));

            mp.collection_partition_ids
                .push(coding::merge_u32(c.collection.id, partition.id));
        }

        return Ok(map
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<MultiplePartitionClient>>());
    }

    async fn select_partition(&self, name: &str, id: &str) -> ASResult<PartitionClient> {
        let c: Arc<CollectionInfo> = self.cache_collection(name).await?;
        let slot = coding::hash_str(id) as u32;
        if c.collection.partitions.len() == 1 {
            let p = &c.partitions[0];
            let pc = PartitionClient {
                addr: p.leader.to_string(),
                collection_id: c.collection.id,
                partition_id: p.id,
                slot: slot,
            };
            return Ok(pc);
        }

        let pid = match c.collection.slots.binary_search(&slot) {
            Ok(i) => i,
            Err(i) => i - 1,
        };

        info!("match pid :{}", pid);

        let p = &c.partitions[pid];

        let p = PartitionClient {
            addr: p.leader.to_string(),
            collection_id: p.collection_id,
            partition_id: p.id,
            slot: slot,
        };
        Ok(p)
    }

    //TODO CACHE ME
    pub async fn cache_collection(&self, name: &str) -> ASResult<Arc<CollectionInfo>> {
        if let Some(c) = self.collection_cache.read().unwrap().get(name) {
            return Ok(c.clone());
        }

        let lock = self
            .lock_cache
            .write()
            .unwrap()
            .entry(name.to_string())
            .or_insert(Arc::new(Mutex::new(0)))
            .clone();
        let _ = lock.lock().unwrap();

        if let Some(c) = self.collection_cache.read().unwrap().get(name) {
            return Ok(c.clone());
        }

        let collection = self.meta_cli.collection_get(None, Some(name)).await?;

        let mut cache_field = HashMap::with_capacity(collection.fields.len());

        collection.fields.iter().for_each(|f| {
            cache_field.insert(f.name().to_string(), f.clone());
        });

        //to load partitions
        let mut partitions = Vec::new();

        for pid in collection.partitions.iter() {
            let partition = self.meta_cli.partition_get(collection.id, *pid).await?;
            partitions.push(partition);
        }

        let c = Arc::new(CollectionInfo {
            collection: collection,
            partitions: partitions,
            fields: cache_field,
        });

        self.collection_cache
            .write()
            .unwrap()
            .insert(name.to_string(), c.clone());
        Ok(c.clone())
    }

    fn check_err_cache(&self, i: usize, cname: &str, e: &ASError) -> bool {
        if i + 1 == RETRY {
            return false;
        }
        match e.code() {
            Code::Timeout | Code::PartitionNotFound => {
                warn!("to remove cache by collection:{}", cname);
                self.collection_cache.write().unwrap().remove(cname);
                true
            }
            _ => false,
        }
    }

    fn check_response_cache(&self, i: usize, cname: &str, e: ASError) -> bool {
        if e == ASError::Success {
            return false;
        }
        self.check_err_cache(i, cname, &e)
    }

    async fn channel_cache(&self, addr: &str) -> ASResult<RpcClient<Channel>> {
        if let Some(channel) = self.channel_cache.read().unwrap().get(addr) {
            return Ok(channel.clone());
        };

        let mut map = self.channel_cache.write().unwrap();
        if let Some(channel) = map.get(addr) {
            return Ok(channel.clone());
        };

        info!("to connect channel addr:{}", addr);

        let client = RpcClient::new(
            Endpoint::from_shared(format!("http://{}", addr))?
                .connect()
                .await?,
        );

        map.insert(addr.to_string(), client.clone());

        Ok(client)
    }
}

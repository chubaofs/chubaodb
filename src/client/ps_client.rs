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
use crate::pserverpb::rpc_client::RpcClient;
use crate::pserverpb::*;
use crate::util::{coding, config, entity::*, error::*};
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tonic::transport::{Channel, Endpoint};

use crate::client::partition_client::*;

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
                    let e = cast_to_err(e);
                    if self.check_err_cache(i, collection_name.as_str(), &e) {
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
                    let e = cast_to_err(e);
                    if self.check_err_cache(i, collection_name.as_str(), &e) {
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

    pub async fn search(
        &self,
        collection_name: &str,
        query: String,
        def_fields: Vec<String>,
        size: u32,
    ) -> ASResult<SearchDocumentResponse> {
        'outer: for i in 0..RETRY {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<SearchDocumentResponse>(10);

            match self.select_collection(collection_name).await {
                Ok(mpl) => {
                    for mp in mpl {
                        let mut tx = tx.clone();
                        let query = query.clone();
                        let def_fields = def_fields.clone();
                        tokio::spawn(async move {
                            match mp.search(query, def_fields, size).await {
                                Ok(resp) => {
                                    if let Err(e) = tx.try_send(resp) {
                                        error!("send result has err:{:?}", e); //TODO: if errr
                                    };
                                }
                                Err(e) => {
                                    let e = cast_to_err(e);
                                    let mut resp = SearchDocumentResponse::default();
                                    resp.code = e.0 as i32;
                                    resp.info = Some(SearchInfo {
                                        error: 1,
                                        success: 0,
                                        message: e.1,
                                    });
                                    if let Err(e) = tx.try_send(resp) {
                                        error!("send result has err:{:?}", e); //TODO: if errr
                                    };
                                }
                            };
                        });
                    }
                }
                Err(e) => {
                    let e = cast_to_err(e);
                    if self.check_err_cache(i, collection_name, &e) {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };

            let empty = |_v: tokio::sync::mpsc::Sender<SearchDocumentResponse>| {};

            empty(tx);

            let mut dist = rx.recv().await.unwrap();

            if dist.code as u16 != SUCCESS
                && self.check_response_cache(
                    i,
                    collection_name,
                    dist.code as u16,
                    dist.info.as_ref().unwrap().message.clone(),
                )
            {
                continue 'outer;
            }

            while let Some(src) = rx.recv().await {
                if src.code as u16 != SUCCESS
                    && self.check_response_cache(
                        i,
                        collection_name,
                        src.code as u16,
                        src.info.as_ref().unwrap().message.clone(),
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

    pub async fn count(&self, collection_name: &str) -> ASResult<CountDocumentResponse> {
        'outer: for i in 0..RETRY {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<CountDocumentResponse>(10);

            match self.select_collection(collection_name).await {
                Ok(mpl) => {
                    for mp in mpl {
                        let mut tx = tx.clone();
                        tokio::spawn(async move {
                            match mp.count().await {
                                Ok(resp) => {
                                    if let Err(e) = tx.try_send(resp) {
                                        error!("send result has err:{:?}", e); //TODO: if errr
                                    };
                                }
                                Err(e) => {
                                    let e = cast_to_err(e);
                                    let mut resp = CountDocumentResponse::default();
                                    resp.code = e.0 as i32;
                                    resp.message = format!(
                                        "partition:{:?} has err:{}",
                                        mp.collection_partition_ids, e.1
                                    );
                                    if let Err(e) = tx.try_send(resp) {
                                        error!("send result has err:{:?}", e); //TODO: if errr
                                    };
                                }
                            };
                        });
                    }
                }
                Err(e) => {
                    let e = cast_to_err(e);
                    if self.check_err_cache(i, collection_name, &e) {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };

            let empty = |_v: tokio::sync::mpsc::Sender<CountDocumentResponse>| {};

            empty(tx);

            let mut dist = rx.recv().await.unwrap();

            if dist.code as u16 != SUCCESS
                && self.check_response_cache(
                    i,
                    collection_name,
                    dist.code as u16,
                    dist.message.clone(),
                )
            {
                continue 'outer;
            }

            while let Some(src) = rx.recv().await {
                if src.code as u16 != SUCCESS
                    && self.check_response_cache(
                        i,
                        collection_name,
                        src.code as u16,
                        src.message.clone(),
                    )
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

        if result.code as u16 != SUCCESS {
            return Err(err_code_box(result.code as u16, result.message));
        }

        Ok(result)
    }

    async fn select_collection(&self, name: &str) -> ASResult<Vec<MultiplePartitionClient>> {
        let c: Arc<CollectionInfo> = self.cache_collection(name).await?;
        let cid = c.collection.id.unwrap();

        let mut map = HashMap::new();

        for partition in c.partitions.iter() {
            let mp = map
                .entry(partition.leader.clone())
                .or_insert(MultiplePartitionClient::new(partition.leader.clone()));

            mp.collection_partition_ids
                .push(coding::merge_u32(cid, partition.id));
        }

        return Ok(map
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<MultiplePartitionClient>>());
    }

    async fn select_partition(&self, name: &str, id: &str) -> ASResult<PartitionClient> {
        let c: Arc<CollectionInfo> = self.cache_collection(name).await?;
        let cid = c.collection.id.unwrap();
        let len = c.collection.partitions.as_ref().unwrap().len();
        let slot = coding::hash_str(id) as u32;
        if len == 1 {
            let p = &c.partitions[0];
            let pc = PartitionClient {
                addr: p.leader.to_string(),
                collection_id: cid,
                partition_id: p.id,
                slot: slot,
            };
            return Ok(pc);
        }

        let pid = match c.collection.slots.as_ref().unwrap().binary_search(&slot) {
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

        let collection = self.meta_cli.get_collection(name).await?;

        let mut cache_field = HashMap::with_capacity(collection.fields.len());

        collection.fields.iter().for_each(|f| {
            cache_field.insert(f.name.as_ref().unwrap().to_string(), f.clone());
        });

        //to load partitions
        let cid = collection.id.unwrap();
        let mut partitions = Vec::new();

        for pid in collection.partitions.as_ref().unwrap() {
            let partition = self.meta_cli.get_partition(cid, *pid).await?;
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

    fn check_err_cache(&self, i: usize, cname: &str, e: &Box<GenericError>) -> bool {
        if i + 1 == RETRY {
            return false;
        }
        match e.0 {
            NOT_FOUND => {
                warn!("to remove cache by collection:{}", cname);
                self.collection_cache.write().unwrap().remove(cname);
                true
            }
            _ => false,
        }
    }

    fn check_response_cache(&self, i: usize, cname: &str, code: u16, msg: String) -> bool {
        if code == SUCCESS {
            return false;
        }
        let e = Box::new(GenericError(code, msg));
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

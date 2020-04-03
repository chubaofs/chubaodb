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
use crate::pserver::simba::simba::Simba;
use crate::pserverpb::*;
use crate::util::{coding, config, entity::*, error::*};
use fp_rust::sync::CountDownLatch;
use log::{error, info};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;
pub struct PartitionService {
    pub server_id: u32,
    pub simba_map: RwLock<HashMap<(u32, u32), Arc<RwLock<Simba>>>>,
    pub conf: Arc<config::Config>,
    pub lock: Mutex<usize>,
    meta_client: Arc<MetaClient>,
}

impl PartitionService {
    pub fn new(conf: Arc<config::Config>) -> Self {
        PartitionService {
            server_id: 0,
            simba_map: RwLock::new(HashMap::new()),
            conf: conf.clone(),
            lock: Mutex::new(0),
            meta_client: Arc::new(MetaClient::new(conf)),
        }
    }

    pub async fn init(&mut self) -> ASResult<()> {
        let ps = match self
            .meta_client
            .heartbeat(
                self.conf.ps.zone_id as u32,
                None,
                self.conf.global.ip.as_str(),
                self.conf.ps.rpc_port as u32,
            )
            .await
        {
            Ok(p) => p,
            Err(e) => {
                let e = cast_to_err(e);
                if e.0 != NOT_FOUND {
                    return Err(e);
                }
                PServer::new(
                    self.conf.ps.zone_id,
                    None,
                    format!("{}:{}", self.conf.global.ip.as_str(), self.conf.ps.rpc_port),
                )
            }
        };
        if ps.id.is_some() {
            self.server_id = ps.id.unwrap();
        }

        info!("get_server line:{:?}", ps);

        for wp in ps.write_partitions {
            if let Err(err) = self
                .init_partition(
                    wp.collection_id,
                    wp.id,
                    wp.replicas,
                    false,
                    wp.version,
                    false,
                )
                .await
            {
                error!("init partition has err:{}", err.to_string());
            };
        }

        self.take_heartbeat().await?;

        Ok(())
    }

    pub async fn init_partition(
        &self,
        collection_id: u32,
        partition_id: u32,
        replicas: Vec<Replica>,
        readonly: bool,
        version: u64,
        wait_for_success: bool,
    ) -> ASResult<()> {
        info!(
            "to load partition:{} partition:{} exisit:{}",
            collection_id,
            partition_id,
            self.simba_map
                .read()
                .unwrap()
                .contains_key(&(collection_id, partition_id))
        );
        let _ = self.lock.lock().unwrap();
        info!("Start init_partition");

        if self
            .simba_map
            .read()
            .unwrap()
            .get(&(collection_id, partition_id))
            .is_some()
        {
            return Ok(());
        }

        let collection = Arc::new(self.meta_client.get_collection_by_id(collection_id).await?);

        if version > 0 {
            self.check_partition_version(collection_id, partition_id, version)
                .await?;
        }

        let partition = Arc::new(Partition {
            id: partition_id,
            collection_id: collection_id,
            replicas: replicas,
            leader: format!("{}:{}", self.conf.global.ip, self.conf.ps.rpc_port),
            version: version + 1,
        });

        let mut latch: Arc<Option<CountDownLatch>> = Arc::new(None);
        if wait_for_success {
            latch = Arc::new(Some(CountDownLatch::new(1)));
        }
        match Simba::new(
            self.conf.clone(),
            readonly,
            collection.clone(),
            partition.clone(),
            self.server_id,
            latch.clone(),
        ) {
            Ok(simba) => {
                self.simba_map
                    .write()
                    .unwrap()
                    .insert((collection_id, partition_id), simba);
            }
            Err(e) => return Err(e),
        };

        if latch.is_some() {
            latch.as_ref().as_ref().unwrap().wait();
        }

        if let Err(e) = self.meta_client.update_partition(&partition).await {
            //if notify master errr, it will rollback
            if let Err(offe) = self.offload_partition(PartitionRequest {
                partition_id: partition.id,
                collection_id: partition.collection_id,
                replicas: vec![],
                readonly: false,
                version: 0,
            }) {
                error!("offload partition:{:?} has err:{:?}", partition, offe);
            };
            return Err(e);
        };

        Ok(())
    }

    async fn check_partition_version(&self, cid: u32, pid: u32, version: u64) -> ASResult<()> {
        let partition = self.meta_client.get_partition(cid, pid).await?;

        if partition.version > version {
            return Err(err_code_box(
                VERSION_ERR,
                format!(
                    "the collection:{} partition:{} version not right expected:{} found:{}",
                    cid, pid, version, partition.version
                ),
            ));
        }
        Ok(())
    }

    //offload partition , if partition not exist , it will return success
    pub fn offload_partition(&self, req: PartitionRequest) -> ASResult<GeneralResponse> {
        info!(
            "to offload partition:{} partition:{} exisit:{}",
            req.collection_id,
            req.partition_id,
            self.simba_map
                .read()
                .unwrap()
                .contains_key(&(req.collection_id, req.partition_id))
        );
        if let Some(store) = self
            .simba_map
            .write()
            .unwrap()
            .remove(&(req.collection_id, req.partition_id))
        {
            store.write().unwrap().stop();
            crate::sleep!(300);
            while Arc::strong_count(&store) > 1 {
                info!(
                    "wait release collection:{} partition:{} now is :{}",
                    req.collection_id,
                    req.partition_id,
                    Arc::strong_count(&store)
                );
                crate::sleep!(300);
            }

            store.write().unwrap().release(); // there use towice for release
        }
        make_general_success()
    }

    pub async fn take_heartbeat(&self) -> ASResult<()> {
        let _ = self.lock.lock().unwrap();

        let wps = self
            .simba_map
            .read()
            .unwrap()
            .values()
            .filter(|s| !s.read().unwrap().readonly())
            .map(|s| (*s.read().unwrap().partition).clone())
            .collect::<Vec<Partition>>();

        match self
            .meta_client
            .put_pserver(&PServer {
                id: Some(self.server_id),
                addr: format!("{}:{}", self.conf.global.ip.as_str(), self.conf.ps.rpc_port),
                write_partitions: wps,
                zone_id: self.conf.ps.zone_id,
                modify_time: 0,
            })
            .await
        {
            Ok(server) => {
                self.server_id = server.id.unwrap();
                return Ok(());
            }
            Err(e) => Err(e),
        }
    }

    pub async fn write(&self, req: WriteDocumentRequest) -> ASResult<GeneralResponse> {
        let store = if let Some(store) = self
            .simba_map
            .read()
            .unwrap()
            .get(&(req.collection_id, req.partition_id))
        {
            store.clone()
        } else {
            return Err(make_not_found_err(req.collection_id, req.partition_id)?);
        };

        store.read().unwrap().write(req);
        make_general_success()
    }

    pub fn get(&self, req: GetDocumentRequest) -> ASResult<DocumentResponse> {
        let store = if let Some(store) = self
            .simba_map
            .read()
            .unwrap()
            .get(&(req.collection_id, req.partition_id))
        {
            store.clone()
        } else {
            make_not_found_err(req.collection_id, req.partition_id)?
        };

        let doc = store
            .read()
            .unwrap()
            .get(req.id.as_str(), req.sort_key.as_str())?;

        Ok(DocumentResponse {
            code: SUCCESS as i32,
            message: String::from("success"),
            doc: doc,
        })
    }

    pub async fn count(&self, req: CountDocumentRequest) -> ASResult<CountDocumentResponse> {
        let mut cdr = CountDocumentResponse {
            code: SUCCESS as i32,
            estimate_count: 0,
            index_count: 0,
            message: String::default(),
        };

        for collection_partition_id in req.cpids.iter() {
            let cpid = coding::split_u32(*collection_partition_id);
            let simba = if let Some(simba) = self.simba_map.read().unwrap().get(&cpid) {
                simba.clone()
            } else {
                return make_not_found_err(cpid.0, cpid.1);
            };

            match simba.clone().read().unwrap().count() {
                Ok(v) => {
                    cdr.estimate_count += v.0;
                    cdr.index_count += v.1;
                }
                Err(e) => {
                    let e = cast_to_err(e);
                    cdr.code = e.0 as i32;
                    cdr.message.push_str(&format!(
                        "collection_partition_id:{} has err:{}  ",
                        collection_partition_id, e.1
                    ));
                }
            }
        }

        return Ok(cdr);
    }

    pub async fn search(&self, sdreq: SearchDocumentRequest) -> ASResult<SearchDocumentResponse> {
        assert_ne!(sdreq.cpids.len(), 0);
        let (tx, rx) = mpsc::channel();

        let sdreq = Arc::new(sdreq);

        for cpid in sdreq.cpids.iter() {
            let cpid = coding::split_u32(*cpid);
            if let Some(simba) = self.simba_map.read().unwrap().get(&cpid) {
                let simba = simba.clone();
                let tx = tx.clone();
                let sdreq = sdreq.clone();
                thread::spawn(move || {
                    tx.send(simba.read().unwrap().search(sdreq)).unwrap();
                });
            } else {
                return make_not_found_err(cpid.0, cpid.1);
            }
        }

        empty(tx);

        let mut dist = rx.recv()?;
        for src in rx {
            dist = merge_search_document_response(dist, src);
        }
        dist.hits.sort_by(|v1, v2| {
            if v1.score >= v2.score {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        });

        if dist.hits.len() > sdreq.size as usize {
            unsafe {
                dist.hits.set_len(sdreq.size as usize);
            }
        }

        Ok(dist)
    }

    pub fn status(&self, _request: GeneralRequest) -> ASResult<GeneralResponse> {
        Ok(GeneralResponse {
            code: SUCCESS as i32,
            message: String::from("ok"),
        })
    }
}

impl PartitionService {
    pub fn command(&self, command: CommandRequest) -> ASResult<Vec<u8>> {
        let value: Value = serde_json::from_slice(command.body.as_slice())?;

        match value["method"].as_str().unwrap() {
            "file_info" => self._file_info(value),
            _ => Err(err_box(format!("not found method:{}", value["method"]))),
        }
    }

    fn _file_info(&self, value: Value) -> ASResult<Vec<u8>> {
        let path = value["path"].as_str().unwrap().to_string();

        let mut result = Vec::new();

        for entry in std::fs::read_dir(path)? {
            let file = convert(entry)?;
            let meta = file.metadata()?;
            result.push(json!({
                "path": file.file_name().into_string(),
                "len":meta.len(),
                "modified": meta.modified().unwrap(),
            }));
        }

        convert(serde_json::to_vec(&result))
    }
}

fn empty(_: mpsc::Sender<SearchDocumentResponse>) {}

fn make_not_found_err<T>(cid: u32, pid: u32) -> ASResult<T> {
    Err(err_code_box(
        NOT_FOUND,
        format!("not found collection:{}  partition by id:{}", cid, pid),
    ))
}

fn make_general_success() -> ASResult<GeneralResponse> {
    Ok(GeneralResponse {
        code: SUCCESS as i32,
        message: String::from("success"),
    })
}

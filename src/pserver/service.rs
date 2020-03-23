// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.
use crate::client::meta_client::MetaClient;
use crate::pserver::simba::simba::{Engine, Simba};
use crate::pserverpb::*;
use crate::util::{coding, config, entity::*, error::*};
use log::{error, info};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;

pub struct PartitionService {
    pub simba_map: RwLock<HashMap<(u32, u32), Arc<Simba>>>,
    pub conf: Arc<config::Config>,
    pub lock: Mutex<usize>,
    meta_client: Arc<MetaClient>,
}

impl PartitionService {
    pub fn new(conf: Arc<config::Config>) -> Self {
        PartitionService {
            simba_map: RwLock::new(HashMap::new()),
            conf: conf.clone(),
            lock: Mutex::new(0),
            meta_client: Arc::new(MetaClient::new(conf)),
        }
    }

    pub async fn init(&self) -> ASResult<()> {
        let ps = match self
            .meta_client
            .heartbeat(
                self.conf.ps.zone_id as u32,
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
                    format!("{}:{}", self.conf.global.ip.as_str(), self.conf.ps.rpc_port),
                )
            }
        };

        info!("get_server line:{:?}", ps);

        for wp in ps.write_partitions {
            if let Err(err) = self
                .init_partition(wp.collection_id, wp.id, false, wp.version)
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
        readonly: bool,
        version: u64,
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

        let collection = self.meta_client.get_collection_by_id(collection_id).await?;

        if version > 0 {
            self.check_partition_version(collection_id, partition_id, version)
                .await?;
        }

        let partition = Partition {
            id: partition_id,
            collection_id: collection_id,
            leader: format!("{}:{}", self.conf.global.ip, self.conf.ps.rpc_port),
            version: version + 1,
        };

        match Simba::new(self.conf.clone(), readonly, &collection, &partition) {
            Ok(simba) => {
                self.simba_map
                    .write()
                    .unwrap()
                    .insert((collection_id, partition_id), simba);
            }
            Err(e) => return Err(e),
        };

        if let Err(e) = self.meta_client.update_partition(&partition).await {
            //if notify master errr, it will rollback
            if let Err(offe) = self.offload_partition(PartitionRequest {
                partition_id: partition.id,
                collection_id: partition.collection_id,
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
            store.release();

            while Arc::strong_count(&store) > 1 {
                info!(
                    "wait release collection:{} partition:{} now is :{}",
                    req.collection_id,
                    req.partition_id,
                    Arc::strong_count(&store)
                );
                thread::sleep(std::time::Duration::from_millis(300));
            }

            store.release(); // there use towice for release
        }
        make_general_success()
    }

    pub async fn take_heartbeat(&self) -> ASResult<()> {
        let _ = self.lock.lock().unwrap();

        let pids = self
            .simba_map
            .read()
            .unwrap()
            .values()
            .filter(|s| !s.readonly())
            .map(|s| s.partition.clone())
            .collect::<Vec<Partition>>();

        self.meta_client
            .put_pserver(&PServer {
                addr: format!("{}:{}", self.conf.global.ip.as_str(), self.conf.ps.rpc_port),
                write_partitions: pids,
                zone_id: self.conf.ps.zone_id,
                modify_time: 0,
            })
            .await
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

        store.write(req).await?;
        make_general_success()
    }

    pub fn get(&self, req: GetDocumentRequest) -> ASResult<DocumentResponse> {
        if let Some(store) = self
            .simba_map
            .read()
            .unwrap()
            .get(&(req.collection_id, req.partition_id))
        {
            Ok(DocumentResponse {
                code: SUCCESS as i32,
                message: String::from("success"),
                doc: store.get(req.id.as_str(), req.sort_key.as_str())?,
            })
        } else {
            make_not_found_err(req.collection_id, req.partition_id)?
        }
    }

    pub async fn count(&self, req: CountDocumentRequest) -> ASResult<CountDocumentResponse> {
        let mut cdr = CountDocumentResponse {
            code: SUCCESS as i32,
            partition_count: HashMap::new(),
            sum: 0,
            message: String::default(),
        };

        for collection_partition_id in req.cpids.iter() {
            let cpid = coding::split_u32(*collection_partition_id);
            if let Some(simba) = self.simba_map.read().unwrap().get(&cpid) {
                match simba.count() {
                    Ok(v) => {
                        cdr.sum += v;
                        cdr.partition_count.insert(*collection_partition_id, v);
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
            } else {
                return make_not_found_err(cpid.0, cpid.1);
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
                    tx.send(simba.search(sdreq)).unwrap();
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

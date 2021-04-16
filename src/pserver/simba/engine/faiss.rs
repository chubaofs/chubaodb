use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::pserver::simba::engine::rocksdb::RocksDB;
use crate::pserverpb::*;
use crate::util::time::current_millis;
use crate::util::{
    coding::field_coding,
    coding::{slice_slice, slice_u32, u32_slice},
    entity::*,
    error::*,
};
use crate::*;
use faiss4rs::{Config, Index};
use tracing::log::{debug, error, info, warn};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU32, Ordering::SeqCst},
    Arc, RwLock,
};
#[derive(PartialEq, Copy, Clone)]
pub enum IndexStatus {
    NotReady,
    Runging,
    Stoping,
    Stoped,
}

const VECTOR_DIR_NAME: &'static str = "vector";
const INDEX_FILE_NAME: &'static str = "index.dat";
const INDEX_FILE_NAME_TMP: &'static str = "index.dat.tmp";

pub struct IndexField {
    pub field: VectorField,
    pub index: RwLock<Index>,
    index_dir: PathBuf,
    flush_count: AtomicU32,
    status: RwLock<IndexStatus>,
}

impl IndexField {
    fn add_with_ids(&self, ids: &Vec<i64>, data: &Vec<f32>) -> ASResult<()> {
        conver(self.index.read().unwrap().add_with_ids(ids, data))
    }

    fn search(&self, queries: &Vec<f32>, size: i32) -> ASResult<(Vec<i64>, Vec<f32>)> {
        let num_query = queries.len() as i32 / self.field.dimension;
        Ok(self.index.read().unwrap().search(size, num_query, queries))
    }

    //return bool about need train model
    fn flush(&self) -> ASResult<()> {
        if self.status.read().unwrap().deref() == &IndexStatus::Runging {
            let count = self.flush_count.load(SeqCst);
            if self.count() - count > 10000 {
                self.flush_count.store(self.count(), SeqCst);
                self._flush()?;
            }
        }

        Ok(())
    }

    fn _flush(&self) -> ASResult<()> {
        info!("field:{} begin flush vector index", self.field.name);
        let now = current_millis();
        self.index.write().unwrap().write_index();
        let tmp = self
            .index_dir
            .join(format!("{}_{}", self.field.name, INDEX_FILE_NAME_TMP));
        let effe = self
            .index_dir
            .join(format!("{}_{}", self.field.name, INDEX_FILE_NAME));
        std::fs::rename(tmp, effe)?;
        info!(
            "field:{} begin flush vector index ok use time:{}",
            self.field.name,
            (current_millis() - now)
        );
        Ok(())
    }

    fn stop(&self, sync: bool) {
        {
            let mut stop = self.status.write().unwrap();
            if stop.deref() != &IndexStatus::Stoped {
                *stop = IndexStatus::Stoping;
            }
        }

        if let Err(e) = self._flush() {
            error!("field:{} flush has err:{}", self.field.name, e.to_string());
        };

        if sync {
            for _ in 0..100 {
                crate::sleep!(1000);
                if self.status() == IndexStatus::Stoped {
                    info!("stop field:{} index has stoped.", self.field.name);
                    return;
                }
            }

            error!("stop field:{} index has time out.", self.field.name);
        }
    }

    fn status(&self) -> IndexStatus {
        *self.status.read().unwrap().deref()
    }

    pub fn max_id(&self) -> u32 {
        self.index.read().unwrap().max_id() as u32
    }

    pub fn count(&self) -> u32 {
        self.index.read().unwrap().count() as u32
    }
}

pub struct Faiss {
    base: Arc<BaseEngine>,
    pub fields: HashMap<String, Arc<IndexField>>,
}

impl Deref for Faiss {
    type Target = Arc<BaseEngine>;
    fn deref<'a>(&'a self) -> &'a Arc<BaseEngine> {
        &self.base
    }
}

impl crate::util::entity::MetricType {
    pub fn to_faiss(&self) -> faiss4rs::MetricType {
        match self {
            MetricType::L2 => faiss4rs::MetricType::L2,
            MetricType::InnerProduct => faiss4rs::MetricType::InnerProduct,
        }
    }
}

impl Faiss {
    pub fn new(db: Arc<RocksDB>, base: Arc<BaseEngine>) -> ASResult<Faiss> {
        let mut faiss = Faiss {
            base: base.clone(),
            fields: HashMap::new(),
        };
        for i in faiss.base.collection.vector_field.iter() {
            let f = base.collection.fields[*i as usize].vector()?;

            let index_dir = base.base_path().join(Path::new(VECTOR_DIR_NAME));
            if !index_dir.exists() {
                std::fs::create_dir_all(&index_dir)?;
            }

            let mut conf = Config {
                dimension: f.dimension,
                description: f.description.clone(),
                metric_type: f.metric_type.to_faiss(),
                path: index_dir
                    .clone()
                    .join(format!("{}_{}", f.name, INDEX_FILE_NAME))
                    .to_str()
                    .unwrap()
                    .to_string(),
            };

            let (status, index) = if std::path::Path::new(conf.path.as_str()).is_file() {
                (IndexStatus::Runging, Index::open_or_create(conf))
            } else {
                conf.description = String::from("Flat");
                (IndexStatus::NotReady, Index::new(conf))
            };

            let count = index.count() as u32;
            let index = Arc::new(IndexField {
                field: f.clone(),
                index: RwLock::new(index),
                index_dir: index_dir,
                flush_count: AtomicU32::new(count),
                status: RwLock::new(status),
            });

            let suffix = field_coding(&f.name, u32::max_value());

            let dimension = index.field.dimension as usize;
            let is_array = index.field.array;
            let batch_size = 1000;
            let mut buf: Vec<f32> = Vec::with_capacity(batch_size);
            let mut ids: Vec<i64> = Vec::with_capacity(dimension * batch_size * 4);

            loop {
                let temp = &mut buf;
                let ids = &mut ids;
                unsafe {
                    ids.set_len(0);
                    temp.set_len(0);
                }

                read_vector_buffer(
                    &db,
                    field_coding(&f.name, index.max_id() + 1),
                    suffix.as_slice(),
                    ids,
                    temp,
                    dimension,
                    is_array,
                )?;

                if ids.len() == 0 {
                    break;
                }

                if let Err(e) = index.add_with_ids(ids, temp) {
                    error!("index with ids has err:{:?}", e);
                    continue;
                };
            }
            {
                let db = db.clone();
                let index = index.clone();
                let status = status.clone();
                std::thread::spawn(move || {
                    let name = index.field.name.clone();
                    if status == IndexStatus::Runging {
                        Faiss::index_job_by_index(db, index);
                        warn!("field:{:?} stop index_job_by_index", name);
                    } else if status == IndexStatus::NotReady {
                        Faiss::index_job(db, index);
                        warn!("field:{:?} stop index_job", name);
                    }
                });
            }

            faiss.fields.insert(f.name.clone(), index);
        }
        Ok(faiss)
    }

    pub fn search(
        &self,
        sdreq: Arc<QueryRequest>,
        _bitmap: Option<RoaringBitmap>,
        total: u64,
    ) -> ASResult<SearchDocumentResponse> {
        if total == 0 {
            return Ok(SearchDocumentResponse {
                code: Code::Success as i32,
                total: total,
                hits: Vec::new(),
                info: None, //if this is none means it is success
            });
        }

        if let Some(vq) = sdreq.vector_query.as_ref() {
            let (ids, scores) = self
                .get_field(vq.field.as_str())?
                .search(&vq.vector, sdreq.size as i32)?;

            let mut sdr = SearchDocumentResponse {
                code: Code::Success as i32,
                total: total,
                hits: Vec::with_capacity(ids.len()),
                info: None, //if this is none means it is success
            };

            for (i, id) in ids.into_iter().enumerate() {
                sdr.hits.push(Hit {
                    collection_name: self.collection.name.clone(),
                    score: scores[i],
                    doc: u32_slice(id as u32).to_vec(),
                });
            }
            return Ok(sdr);
        }
        return result_def!("impossible");
    }

    pub fn get_field(&self, name: &str) -> ASResult<Arc<IndexField>> {
        match self.fields.get(name) {
            Some(i) => Ok(i.clone()),
            None => result_def!("the field:{} not have vector index", name),
        }
    }

    fn index_job(db: Arc<RocksDB>, index: Arc<IndexField>) {
        warn!("field:{:?} start index_job ", index.field.name);
        db.arc_count.fetch_add(1, SeqCst);
        let field_name = index.field.name.as_str();
        let suffix = field_coding(field_name, u32::max_value());

        let is_array = index.field.array;
        let dimension = index.field.dimension as usize;
        let train_size = index.field.train_size as usize;

        let max_len = (train_size * dimension) as usize;
        let mut buf: Vec<f32> = Vec::with_capacity(max_len);
        let mut ids: Vec<i64> = Vec::with_capacity(train_size as usize);
        let once = std::sync::Once::new();

        while index.status() == IndexStatus::NotReady {
            let temp = &mut buf;
            let ids = &mut ids;
            unsafe {
                ids.set_len(0);
                temp.set_len(0);
            }

            if train_size > 0 && index.count() > train_size as u32 {
                let db = db.clone();
                let index = index.clone();
                let name = index.field.name.clone();

                once.call_once(|| {
                    std::thread::spawn(move || {
                        db.arc_count.fetch_sub(1, SeqCst);
                        Faiss::index_job_by_trian_index(db, index);
                        warn!("field:{:?} stop index_job_by_trian_index ", name);
                    });
                });
            }

            let result = read_vector_buffer(
                &db,
                field_coding(field_name, index.max_id() + 1),
                suffix.as_slice(),
                ids,
                temp,
                dimension,
                is_array,
            );

            if let Err(e) = result {
                error!("create index has err:{:?}", e);
                crate::sleep!(3000);
                continue;
            }
            if ids.len() == 0 {
                debug!("field:{} no document need index so skip", field_name);
                crate::sleep!(3000);
                continue;
            }
            if let Err(e) = index.add_with_ids(ids, temp) {
                error!("index with ids has err:{:?}", e);
                crate::sleep!(3000);
                continue;
            };
            if ids.len() < train_size {
                crate::sleep!(3000);
            }
        }
    }

    fn index_job_by_trian_index(db: Arc<RocksDB>, index: Arc<IndexField>) {
        warn!(
            "field:{:?} start index_job_by_trian_index",
            index.field.name
        );
        db.arc_count.fetch_add(1, SeqCst);
        let field_name = index.field.name.clone();

        let max_len = (index.field.train_size * index.field.dimension) as usize;

        let mut buf: Vec<f32> = Vec::with_capacity(max_len);
        let faiss_index = Index::new(Config {
            dimension: index.field.dimension,
            description: index.field.description.clone(),
            metric_type: index.field.metric_type.to_faiss(),
            path: index.index.read().unwrap().config.path.clone(),
        });

        let prefix = field_coding(&field_name, 1);
        let suffix = field_coding(&field_name, u32::max_value());
        let temp = &mut buf;
        let result = db.prefix_range(prefix, |k, v| -> ASResult<bool> {
            if k >= suffix.as_slice() {
                return Ok(false);
            }

            temp.extend_from_slice(slice_slice(v));

            if temp.len() >= max_len {
                return Ok(false);
            }
            return Ok(true);
        });

        if let Err(e) = result {
            error!("create index has err:{:?}", e);
        }

        let now = current_millis();
        info!("begin train index for field:{}", field_name);
        if let Err(e) = faiss_index.train(temp) {
            error!("field:{} train index has err:{:?}", field_name, e);
            return;
        };
        info!(
            "field:{} train use time:{}",
            field_name,
            (current_millis() - now)
        );
        unsafe {
            temp.set_len(0);
        }

        let is_array = index.field.array;
        let dimension = index.field.dimension as usize;
        let train_size = index.field.train_size as usize;

        let mut ids: Vec<i64> = Vec::with_capacity(train_size as usize);
        while index.status() != IndexStatus::Stoping {
            let temp = &mut buf;
            let ids = &mut ids;
            unsafe {
                ids.set_len(0);
                temp.set_len(0);
            }

            let result = read_vector_buffer(
                &db,
                field_coding(&field_name, index.max_id() + 1),
                suffix.as_slice(),
                ids,
                temp,
                dimension,
                is_array,
            );

            if let Err(e) = result {
                error!("create index has err:{:?}", e);
                crate::sleep!(3000);
                continue;
            }

            if ids.len() == 0 {
                break;
            }

            if let Err(e) = faiss_index.add_with_ids(ids, temp) {
                error!("index with ids has err:{:?}", e);
                crate::sleep!(3000);
                continue;
            };
            //if id is over goto index_job_by_index
            if ids.len() < train_size {
                break;
            }
        }

        {
            let mut i = index.index.write().unwrap();
            *index.status.write().unwrap() = IndexStatus::Runging;
            *i = faiss_index;
        }

        std::thread::spawn(move || {
            db.arc_count.fetch_sub(1, SeqCst);
            Faiss::index_job_by_index(db, index);
            warn!("field:{:?} stop index_job_by_index ", field_name);
        });
    }

    fn index_job_by_index(db: Arc<RocksDB>, index: Arc<IndexField>) {
        warn!("field:{:?} start index_job_by_index ", index.field.name);
        db.arc_count.fetch_add(1, SeqCst);
        let field_name = index.field.name.as_str();
        let suffix = field_coding(field_name, u32::max_value());

        let max_len = (index.field.train_size * index.field.dimension) as usize;

        let mut buf: Vec<f32> = Vec::with_capacity(max_len);

        let is_array = index.field.array;
        let dimension = index.field.dimension as usize;
        let train_size = index.field.train_size as usize;

        let mut ids: Vec<i64> = Vec::with_capacity(train_size as usize);
        while index.status() != IndexStatus::Stoping {
            let temp = &mut buf;
            let ids = &mut ids;
            unsafe {
                ids.set_len(0);
                temp.set_len(0);
            }
            let result = read_vector_buffer(
                &db,
                field_coding(&field_name, index.max_id() + 1),
                suffix.as_slice(),
                ids,
                temp,
                dimension,
                is_array,
            );

            if let Err(e) = result {
                error!("create index has err:{:?}", e);
                crate::sleep!(3000);
                continue;
            }

            if ids.len() == 0 {
                debug!("field:{} no document need index so skip", field_name);
                crate::sleep!(3000);
                continue;
            }

            if let Err(e) = index.add_with_ids(ids, temp) {
                error!("index with ids has err:{:?}", e);
                crate::sleep!(3000);
                continue;
            };

            if ids.len() < train_size {
                crate::sleep!(3000);
                continue;
            }
        }

        {
            *index.status.write().unwrap() = IndexStatus::Stoped;
        }
    }
}

impl Engine for Faiss {
    fn flush(&self) -> ASResult<()> {
        for (_, fi) in self.fields.iter() {
            if let Err(e) = fi.flush() {
                info!(
                    "field:{} flush vector index has err:{}",
                    fi.field.name,
                    e.to_string()
                );
            }
        }
        Ok(())
    }

    fn release(&self) {
        for (_f, i) in self.fields.iter() {
            i.stop(false);
        }

        //take twoice
        for (_f, i) in self.fields.iter() {
            i.stop(true);
        }

        if let Err(e) = self.flush() {
            error!("flush faiss engine has err:{:?}", e);
        };
    }
}

// cache size is ids.capacity()
fn read_vector_buffer(
    db: &Arc<RocksDB>,
    prefix: Vec<u8>,
    suffix: &[u8],
    ids: &mut Vec<i64>,
    temp: &mut Vec<f32>,
    dimension: usize,
    is_array: bool,
) -> ASResult<()> {
    let max_len = ids.capacity();
    db.prefix_range(prefix, |k, v| -> ASResult<bool> {
        if k >= suffix {
            return Ok(false);
        }

        let id = slice_u32(&k[k.len() - 4..]);
        if is_array {
            for _i in 0..(v.len() / dimension) as u8 {
                ids.push(id as i64)
            }
        } else {
            ids.push(id as i64);
        }
        temp.extend_from_slice(slice_slice(v));

        if ids.len() >= max_len {
            return Ok(false);
        }
        return Ok(true);
    })
}

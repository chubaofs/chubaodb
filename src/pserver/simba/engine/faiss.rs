use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::pserver::simba::engine::rocksdb::RocksDB;
use crate::util::{
    coding::field_coding,
    coding::{i64_slice, slice_i64, slice_slice},
    entity::*,
    error::*,
    time::Now,
};
use faiss4rs::IndexIVFFlat;
use log::{error, info, warn};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicI64, Ordering::SeqCst},
    Arc, RwLock,
};

#[derive(PartialEq, Copy, Clone)]
pub enum IndexStatus {
    NotReady,
    Training,
    Runging,
    Stoping,
    Stoped,
}

pub struct IndexField {
    pub field: Field,
    pub index: RwLock<IndexIVFFlat>,
    pub dimension: i32,
    train_size: usize,
    max_id: AtomicI64,
    status: RwLock<IndexStatus>,
}

impl IndexField {
    pub fn befor_add(&self, value: &Vec<f32>) -> ASResult<bool> {
        if !self.field.array {
            if value.len() == 0 || value.len() != self.dimension as usize {
                return Err(err_box(format!(
                    "the field:{} vector dimension expectd:{} , found:{}",
                    self.field.name.as_ref().unwrap(),
                    self.dimension,
                    value.len()
                )));
            }
        } else {
            if value.len() % self.dimension as usize != 0 {
                return Err(err_box(format!(
                    "the field:{} vector dimension expectd:{} * n  , found:{}  mod:{}",
                    self.field.name.as_ref().unwrap(),
                    self.dimension,
                    value.len(),
                    value.len() % self.dimension as usize
                )));
            }

            if value.len() / self.dimension as usize > 255 {
                return Err(err_box(format!(
                    "the field:{} vector dimension array size less than 255 found:{}",
                    self.field.name.as_ref().unwrap(),
                    value.len(),
                )));
            }
        }

        Ok(
            self.status.read().unwrap().deref() == &IndexStatus::NotReady
                && self.max_id.load(SeqCst) > self.train_size as i64,
        )
    }

    fn train(&self, data: &Vec<f32>) {
        self.index.read().unwrap().train(data).unwrap();
    }

    fn add_with_ids(&self, ids: &Vec<i64>, data: &Vec<f32>) -> ASResult<()> {
        self.index.read().unwrap().add_with_ids(ids, data)
    }

    fn search(&self, queries: &Vec<f32>, size: i32) -> ASResult<(Vec<i64>, Vec<f32>)> {
        let num_query = queries.len() as i32 / self.dimension;
        Ok(self.index.read().unwrap().search(size, num_query, queries))
    }

    fn stop(&self, sync: bool) {
        {
            let mut stop = self.status.write().unwrap();
            if stop.deref() != &IndexStatus::Stoped {
                *stop = IndexStatus::Stoping;
            }
        }

        if sync {
            for _ in 0..100 {
                crate::sleep!(1000);
                if self.status() == IndexStatus::Stoped {
                    info!(
                        "stop field:{} index has stoped.",
                        self.field.name.as_ref().unwrap()
                    );
                    return;
                }
            }

            error!(
                "stop field:{} index has time out.",
                self.field.name.as_ref().unwrap()
            );
        }
    }

    fn status(&self) -> IndexStatus {
        *self.status.read().unwrap().deref()
    }
}

pub struct Faiss {
    base: Arc<BaseEngine>,
    fields: HashMap<String, Arc<IndexField>>,
}

impl Deref for Faiss {
    type Target = Arc<BaseEngine>;
    fn deref<'a>(&'a self) -> &'a Arc<BaseEngine> {
        &self.base
    }
}

impl Faiss {
    pub fn new(base: Arc<BaseEngine>) -> ASResult<Faiss> {
        let mut faiss = Faiss {
            base: base,
            fields: HashMap::new(),
        };
        for f in faiss.base.collection.fields.iter() {
            if f.internal_type == FieldType::VECTOR {
                let dimension = f.get_option_value(field_option::DIMENSION)?;
                faiss.fields.insert(
                    f.name.as_ref().unwrap().to_string(),
                    Arc::new(IndexField {
                        field: f.clone(),
                        index: RwLock::new(IndexIVFFlat::new(dimension)),
                        train_size: f.get_option_value(field_option::TRAIN_SIZE)?,
                        dimension: dimension,
                        max_id: AtomicI64::new(1),
                        status: RwLock::new(IndexStatus::NotReady),
                    }),
                );
            }
        }
        Ok(faiss)
    }

    pub fn get_field(&self, name: &str) -> ASResult<Arc<IndexField>> {
        match self.fields.get(name) {
            Some(i) => Ok(i.clone()),
            None => Err(err_box(format!("the field:{} not have vector index", name))),
        }
    }

    pub fn start_job(db: Arc<RocksDB>, index: Arc<IndexField>) {
        std::thread::spawn(move || {
            let name = index.field.name.as_ref().unwrap().to_string();
            Faiss::index_job(db, index);
            warn!("field:{:?} stop index job ", name);
        });
    }

    fn index_job(db: Arc<RocksDB>, index: Arc<IndexField>) {
        {
            let mut status = index.status.write().unwrap();
            if status.deref() == &IndexStatus::NotReady {
                *status = IndexStatus::Training;
            } else {
                warn!("field is [:?] so skip"); //TODO: FIX WHEN REBUILD
                return;
            }
        }
        warn!("field:{:?} start index job ", index.field.name);

        let field_name = index.field.name.as_ref().unwrap();

        let prefix = field_coding(field_name, index.max_id.load(SeqCst) + 1);
        let suffix = field_coding(field_name, i64::MAX);

        let now = Now::new();

        let max_len = index.train_size * index.dimension as usize;

        let mut buf: Vec<f32> = Vec::with_capacity(max_len);

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
        index.train(temp);

        unsafe {
            temp.set_len(0);
        }

        info!(
            "field:{} train ok, use time:{}",
            index.field.name.as_ref().unwrap(),
            now.use_time_str()
        );

        let is_array = index.field.array;
        let dimension = index.dimension as usize;

        let mut ids: Vec<i64> = Vec::with_capacity(index.train_size);
        while index.status() != IndexStatus::Stoping {
            let temp = &mut buf;
            let ids = &mut ids;
            let prefix = field_coding(field_name, index.max_id.load(SeqCst) + 1);

            let result = db.prefix_range(prefix, |k, v| -> ASResult<bool> {
                if k >= suffix.as_slice() {
                    return Ok(false);
                }

                let id = slice_i64(&k[k.len() - 8..]);

                if is_array {
                    //need multiple id general
                    for i in 0..(v.len() / dimension) as u8 {
                        ids.push(make_index_id(i, id))
                    }
                } else {
                    ids.push(id);
                }

                temp.extend_from_slice(slice_slice(v));

                if temp.len() >= max_len {
                    return Ok(false);
                }

                return Ok(true);
            });

            if let Err(e) = result {
                error!("create index has err:{:?}", e);
                crate::sleep!(3000);
                continue;
            }

            if let Err(e) = index.add_with_ids(ids, temp) {
                error!("index with ids has err:{:?}", e);
                crate::sleep!(3000);
                continue;
            };

            index.max_id.store(ids[ids.len() - 1], SeqCst);

            if ids.len() < index.train_size {
                crate::sleep!(3000);
            }

            unsafe {
                ids.set_len(0);
                temp.set_len(0);
            }
        }

        {
            *index.status.write().unwrap() = IndexStatus::Stoped;
        }
    }
}

impl Engine for Faiss {
    fn flush(&self) -> ASResult<()> {
        Ok(())
    }

    fn release(&self) {
        for (f, i) in self.fields.iter() {
            i.stop(false);
        }

        //take twoice
        for (f, i) in self.fields.iter() {
            i.stop(true);
        }

        if let Err(e) = self.flush() {
            error!("flush faiss engine has err:{:?}", e);
        };
    }
}

fn make_index_id(index: u8, id: i64) -> i64 {
    let mut s = i64_slice(id);
    s[0] = index;
    slice_i64(&s)
}

#[test]
fn test_make_index_id() {
    let id = 999;
    let index = 100;

    let result = make_index_id(index, id);
    println!("{:?}", result);
    println!("{:?}", i64_slice(id));
    println!("{:?}", i64_slice(result));

    assert_eq!(7205759403792794599, result);
}

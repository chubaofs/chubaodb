use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::util::{entity::*, error::*};
use faiss4rs::IndexIVFFlat;
use log::{error, info, warn};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicI64, Ordering::SeqCst},
    Arc, RwLock,
};

#[derive(PartialEq)]
pub enum IndexStatus {
    NotReady,
    Training,
    Runging,
}

pub struct IndexField {
    pub field: Field,
    pub index: RwLock<Option<IndexIVFFlat>>,
    pub dimension: i32,
    train_size: usize,
    value_count: AtomicI64,
    status: RwLock<IndexStatus>, // 0 not index , 1 . training  . 2 . train ok
}

impl IndexField {
    pub fn increment(&self) -> i64 {
        self.value_count.fetch_add(1, SeqCst)
    }

    pub fn decrement(&self) -> i64 {
        self.value_count.fetch_add(-1, SeqCst)
    }

    pub fn get_value_count(&self) -> i64 {
        self.value_count.load(SeqCst)
    }

    pub fn validate(&self, value: &Vec<f32>) -> ASResult<bool> {
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
        }

        Ok(
            self.status.read().unwrap().deref() == &IndexStatus::NotReady
                && self.get_value_count() > self.train_size as i64,
        )
    }
}

pub struct Faiss {
    base: Arc<BaseEngine>,
    fields: HashMap<String, IndexField>,
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
                faiss.fields.insert(
                    f.name.as_ref().unwrap().to_string(),
                    IndexField {
                        field: f.clone(),
                        index: RwLock::new(None),
                        train_size: f.get_option_value(field_option::TRAIN_SIZE)?,
                        dimension: f.get_option_value(field_option::DIMENSION)?,
                        value_count: AtomicI64::new(1),
                        status: RwLock::new(IndexStatus::NotReady),
                    },
                );
            }
        }
        Ok(faiss)
    }

    pub fn get_field(&self, name: &str) -> ASResult<&IndexField> {
        let index_field = self.fields.get(name);
        if index_field.is_none() {
            return Err(err_box(format!("the field:{} not have vector index", name)));
        }
        Ok(index_field.unwrap())
    }
}

impl Engine for Faiss {
    fn flush(&self) -> ASResult<()> {
        Ok(())
    }

    fn release(&self) {}
}

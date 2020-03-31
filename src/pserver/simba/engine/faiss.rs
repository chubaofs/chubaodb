use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::util::{entity::*, error::*};
use faiss4rs::*;
use std::collections::HashMap;
use std::ops::Deref;

struct IndexField {
    field: Field,
    index: IndexIVFFlat,
}

pub struct Faiss {
    base: BaseEngine,
    fields: HashMap<String, IndexField>,
}

impl Deref for Faiss {
    type Target = BaseEngine;
    fn deref<'a>(&'a self) -> &'a BaseEngine {
        &self.base
    }
}

impl Faiss {
    pub fn new(base: BaseEngine) -> ASResult<Faiss> {
        let mut faiss = Faiss {
            base: base,
            fields: HashMap::new(),
        };
        for f in faiss.base.collection.fields.iter() {
            if f.internal_type == FieldType::VECTOR {
                let dimension = f.get_option_value("dimension");

                faiss.fields.insert(
                    f.name.as_ref().unwrap().to_string(),
                    IndexField {
                        field: f.clone(),
                        index: IndexIVFFlat::new(),
                    },
                );
            }
        }
        Ok(faiss)
    }
}

impl Engine for Faiss {
    fn flush(&self) -> ASResult<()> {
        Ok(())
    }

    fn release(&self) {}
}

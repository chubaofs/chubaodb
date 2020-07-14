use crate::pserver::simba::engine::rocksdb::RocksDB;
use crate::pserverpb::*;
use crate::util::{
    coding::{slice_f64, slice_i64, sort_coding::*},
    entity::{Field, ID_BYTES},
    error::*,
};
use crate::*;
use log::error;
use std::sync::Arc;

#[derive(Clone)]
pub enum Function {
    Count(Count),
    Stats(Stats),
    Hits(Hits),
}

//TODO: FIX it by into
impl Function {
    pub fn make_agg_value(&self, db: Option<&Arc<RocksDB>>) -> AggValue {
        match self {
            Function::Count(c) => AggValue {
                agg_value: Some(agg_value::AggValue::Count(c.result.clone())),
            },
            Function::Stats(s) => AggValue {
                agg_value: Some(agg_value::AggValue::Stats(s.result.clone())),
            },
            Function::Hits(h) => {
                if db.is_none() {
                    return AggValue {
                        agg_value: Some(agg_value::AggValue::Hits(h.result.clone())),
                    };
                }

                let db = db.unwrap();

                let mut hits = Vec::with_capacity(h.result.hits.len());
                for hit in h.result.hits.iter() {
                    let mut hit = hit.clone();
                    match db.get_doc_by_id(&hit.doc) {
                        Ok(v) => match v {
                            Some(v) => {
                                hit.doc = v;
                                hits.push(hit);
                            }
                            None => error!("not found doc by id :{:?}", &hit.doc),
                        },
                        Err(e) => error!("find doc by id :{:?} has err:{:?}", &hit.doc, e),
                    }
                }
                AggValue {
                    agg_value: Some(agg_value::AggValue::Hits(AggHits {
                        size: h.result.size as u64,
                        count: h.result.count,
                        hits: hits,
                    })),
                }
            }
        }
    }
}

impl Function {
    pub fn new(
        name: String,
        params: Vec<String>,
        collection_name: String,
        field: Field,
    ) -> ASResult<Function> {
        let fun = match name.as_str() {
            "hits" => {
                let hit_agg = match params.len() {
                    0 => Hits::new(collection_name, 20, field),
                    1 => Hits::new(
                        collection_name,
                        params[0].parse().map_err(|e| {
                            err!(
                                Code::ParamError,
                                "hits format not right, example hits(20) , example:{}",
                                e
                            )
                        })?,
                        field,
                    ),
                    _ => {
                        return result!(
                            Code::ParamError,
                            "hits format not right, example hits(20) , param need number"
                        );
                    }
                };
                Function::Hits(hit_agg)
            }
            "stats" => {
                let stats_agg = match params.len() {
                    1 => Stats::new(params[0].clone(), field),
                    _ => {
                        return result!(
                            Code::ParamError,
                            "stats format not right, example stats(name) , param need field name"
                        );
                    }
                };
                Function::Stats(stats_agg)
            }
            _ => return result!(Code::ParamError, "fun:{} not define", name),
        };

        Ok(fun)
    }

    pub fn name(&self) -> &str {
        match self {
            Function::Count(_) => ID_BYTES,
            Function::Stats(a) => a.result.field.as_str(),
            Function::Hits(_) => ID_BYTES,
        }
    }

    pub fn key(&self) -> String {
        match self {
            Function::Count(_) => String::from("count"),
            Function::Stats(a) => format!("stats({})", a.result.field),
            Function::Hits(_) => String::from("hits"),
        }
    }

    pub fn map(&mut self, v: &[u8]) -> ASResult<bool> {
        match self {
            Function::Count(a) => {
                a.map()?;
                Ok(true)
            }
            Function::Stats(a) => {
                if v.len() == 0 {
                    a.map(None)?;
                    return Ok(true);
                }

                let array = a.field.array();
                match a.field {
                    Field::int(_) | Field::date(_) => {
                        if array {
                            for v in i64_arr_decoding(v) {
                                a.map(Some(v as f64))?;
                            }
                        } else {
                            a.map(Some(slice_i64(v) as f64))?;
                        }
                    }
                    Field::float(_) => {
                        if array {
                            for v in f64_arr_decoding(v) {
                                a.map(Some(v))?;
                            }
                        } else {
                            a.map(Some(slice_f64(v)))?;
                        }
                    }
                    _ => {
                        return result!(
                            Code::ParamError,
                            "field:{} not support stats agg",
                            a.field.name()
                        );
                    }
                }
                Ok(true)
            }
            Function::Hits(a) => a.map(v.to_vec()),
        }
    }
}

#[derive(Clone)]
pub struct Count {
    pub result: AggCount,
}

impl Count {
    pub fn new() -> Self {
        Self {
            result: AggCount::default(),
        }
    }

    pub fn map(&mut self) -> ASResult<bool> {
        self.result.count += 1;
        Ok(true)
    }
}

#[derive(Clone)]
pub struct Stats {
    pub result: AggStats,
    pub field: Field,
}

impl Stats {
    pub fn new(name: String, field: Field) -> Self {
        Self {
            field,
            result: AggStats {
                field: name,
                count: 0,
                max: f64::MIN,
                min: f64::MAX,
                sum: 0f64,
                missing: 0,
            },
        }
    }

    pub fn map(&mut self, value: Option<f64>) -> ASResult<bool> {
        if value.is_none() {
            self.result.missing += 1;
            return Ok(true);
        }
        let value = value.unwrap();
        let result = &mut self.result;
        result.count += 1;
        if value > result.max {
            result.max = value;
        }

        if value < result.min {
            result.min = value;
        }

        result.sum += value;

        Ok(true)
    }
}

#[derive(Clone)]
pub struct Hits {
    pub result: AggHits,
    pub collection_name: String,
}

impl Hits {
    pub fn new(collection_name: String, size: usize, _: Field) -> Self {
        Self {
            result: AggHits {
                size: size as u64,
                count: 0,
                hits: Vec::with_capacity(size),
            },
            collection_name,
        }
    }

    pub fn map(&mut self, value: Vec<u8>) -> ASResult<bool> {
        if self.result.hits.len() > self.result.size as usize {
            return Ok(false);
        }
        let result = &mut self.result;
        result.hits.push(Hit {
            collection_name: self.collection_name.clone(),
            score: 1f32,
            doc: value,
            sort: vec![],
        });
        Ok(true)
    }
}

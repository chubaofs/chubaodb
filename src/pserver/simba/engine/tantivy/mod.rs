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
mod aggregation_collector;
pub mod bitmap_collector;
pub mod sort;

use crate::pserver::simba::aggregation::Aggregator;
use crate::pserver::simba::engine::{
    engine::{BaseEngine, Engine},
    rocksdb::RocksDB,
};
use crate::pserverpb::*;
use crate::util::coding::{
    f32_slice, iid_coding,
    sort_coding::{f64_arr_coding, i64_arr_coding, str_arr_coding},
};
use crate::util::{
    convert::*,
    entity::{Field::*, ID_BYTES},
    error::*,
};
use crate::*;
use chrono::prelude::*;
use log::{debug, error, info, warn};
use roaring::RoaringBitmap;
use std::convert::TryInto;
use std::{
    fs,
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex, RwLock,
    },
    time::SystemTime,
};
use tantivy::{
    collector::{Count, MultiCollector, TopDocs},
    directory::MmapDirectory,
    query::{QueryParser, TermQuery},
    schema,
    schema::{Field, FieldType, FieldValue, IndexRecordOption, Schema, Value},
    Document, Index, IndexReader, IndexWriter, ReloadPolicy, Term,
};

const INDEXER_MEMORY_SIZE: usize = 1_000_000_000;
const INDEXER_THREAD: usize = 1;
const ID: &'static str = "_iid";
const ID_INDEX: u32 = 0;
const ID_BYTES_INDEX: u32 = 1;
const INDEX_DIR_NAME: &'static str = "index";

pub enum Event {
    Delete(u32),
    // Update(old_iid , new_iid)
    Update(u32, u32),
    Stop,
}

pub struct Tantivy {
    base: Arc<BaseEngine>,
    index: Index,
    index_writer: RwLock<IndexWriter>,
    index_reader: IndexReader,
    field_num: usize,
    db: Arc<RocksDB>,
    tx: Mutex<Sender<Event>>,
    status: AtomicU32,
}

impl Deref for Tantivy {
    type Target = Arc<BaseEngine>;
    fn deref<'a>(&'a self) -> &'a Arc<BaseEngine> {
        &self.base
    }
}

impl Tantivy {
    pub fn new(db: Arc<RocksDB>, base: Arc<BaseEngine>) -> ASResult<Arc<Tantivy>> {
        let now = SystemTime::now();

        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field(ID, schema::IntOptions::default().set_indexed());
        schema_builder.add_bytes_field(ID_BYTES); //if you want put default filed mut modify validate method - 2 in code

        for i in base.collection.scalar_field_index.iter() {
            let field = &base.collection.fields[*i as usize];

            let name = field.name();

            match field {
                int(_) => {
                    schema_builder.add_i64_field(name, schema::IntOptions::default().set_indexed());
                }
                float(_) => {
                    schema_builder.add_f64_field(name, schema::IntOptions::default().set_indexed());
                }
                string(_) => {
                    schema_builder.add_text_field(name, schema::STRING);
                }
                text(_) => {
                    schema_builder.add_text_field(name, schema::TEXT);
                }
                date(_) => {
                    schema_builder
                        .add_date_field(name, schema::IntOptions::default().set_indexed());
                }
                _ => return result_def!("thie type:{:?} can not make index", field),
            }

            if field.value() {
                schema_builder.add_bytes_field(Self::value_field_format(name).as_str());
            }
        }

        let schema = schema_builder.build();
        let field_num = schema.fields().count();

        let index_dir = base.base_path().join(Path::new(INDEX_DIR_NAME));
        if !index_dir.exists() {
            fs::create_dir_all(&index_dir)?;
        }

        let index = conver(Index::open_or_create::<MmapDirectory>(
            MmapDirectory::open(index_dir.to_str().unwrap())?,
            schema,
        ))?;

        let index_writer = index
            .writer_with_num_threads(INDEXER_THREAD, INDEXER_MEMORY_SIZE)
            .unwrap();

        let index_reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()
            .unwrap();

        let (tx, rx) = channel::<Event>();

        db.arc_count.fetch_add(1, SeqCst);
        let tantivy = Arc::new(Tantivy {
            base,
            index,
            index_writer: RwLock::new(index_writer),
            index_reader,
            field_num,
            db,
            tx: Mutex::new(tx),
            status: AtomicU32::new(0),
        });

        Tantivy::start_job(tantivy.clone(), rx);

        info!(
            "init index by collection:{} partition:{} success , use time:{:?} ",
            tantivy.collection.id,
            tantivy.partition.id,
            SystemTime::now().duration_since(now).unwrap().as_millis(),
        );

        Ok(tantivy)
    }

    pub fn value_field_format(name: &str) -> String {
        if name == ID_BYTES {
            return String::from(ID_BYTES);
        }
        format!("__{}", name)
    }

    pub fn release(&self) {
        warn!("partition:{} index released", self.partition.id);
    }

    pub fn count(&self) -> ASResult<u64> {
        let searcher = self.index_reader.searcher();
        let mut sum = 0;
        for sr in searcher.segment_readers() {
            sum += sr.num_docs() as u64;
        }
        Ok(sum)
    }

    pub fn filter(&self, sdr: Arc<QueryRequest>) -> ASResult<(Option<RoaringBitmap>, u64)> {
        if sdr.query == "*" {
            return Ok((None, self.count()?));
        }

        self.check_index()?;
        let searcher = self.index_reader.searcher();
        let query_parser = QueryParser::for_index(
            &self.index,
            sdr.def_fields
                .iter()
                .map(|s| self.index.schema().get_field(s).unwrap())
                .collect(),
        );
        let q = conver(query_parser.parse_query(sdr.query.as_str()))?;
        let result = conver(searcher.search(&q, &bitmap_collector::Bitmap))?;
        let len = result.len();
        Ok((Some(result), len))
    }

    pub fn agg(&self, sdr: Arc<QueryRequest>) -> ASResult<AggregationResponse> {
        self.check_index()?;
        let searcher = self.index_reader.searcher();
        let query_parser = QueryParser::for_index(
            &self.index,
            sdr.def_fields
                .iter()
                .map(|s| self.index.schema().get_field(s).unwrap())
                .collect(),
        );
        let q = conver(query_parser.parse_query(sdr.query.as_str()))?;

        let agg = Aggregator::new(
            &self.collection,
            sdr.size as usize,
            sdr.group.as_str(),
            sdr.fun.as_str(),
        )?;

        let schema = self.index.schema();

        let mut group_fields = Vec::with_capacity(agg.groups.len());
        for g in agg.groups.iter() {
            let field = schema
                .get_field(Self::value_field_format(g.name()).as_str())
                .ok_or_else(|| err!(Code::ParamError, "not found field:{} by group", g.name()))?;
            group_fields.push(field);
        }

        let mut fun_fields = Vec::with_capacity(agg.functions.len());
        for f in agg.functions.iter() {
            let field = schema
                .get_field(Self::value_field_format(f.name()).as_str())
                .ok_or_else(|| err!(Code::ParamError, "not found field:{} by fun", f.name()))?;
            fun_fields.push(field);
        }

        let agg = Arc::new(RwLock::new(agg));

        let collector =
            aggregation_collector::Aggregation::new("-", agg.clone(), group_fields, fun_fields);

        conver(searcher.search(&q, &collector))?;
        let count = agg.read().unwrap().count;

        let result = agg.write().unwrap().make_vec(&sdr, &self.db)?;

        Ok(AggregationResponse {
            code: Code::Success as i32,
            total: count,
            size: result.len() as u32,
            info: None,
            result: result,
        })
    }

    pub fn query(&self, sdr: Arc<QueryRequest>) -> ASResult<SearchDocumentResponse> {
        self.check_index()?;
        let searcher = self.index_reader.searcher();
        let schema = self.index.schema();
        let query_parser = QueryParser::for_index(
            &self.index,
            sdr.def_fields
                .iter()
                .map(|s| schema.get_field(s).unwrap())
                .collect(),
        );
        let size = sdr.size as usize;
        if size == 0 {
            return result!(Code::ParamError, "query size can not to zero");
        }
        let q = conver(query_parser.parse_query(sdr.query.as_str()))?;

        let sort_len = sdr.sort.len() > 0;

        let mut collectors = MultiCollector::new();

        let sort_top_docs_handle = if sort_len {
            let mut field_sorts = sort::FieldSorts::new(sdr.sort.len());
            for s in &sdr.sort {
                let schema_field = schema.get_field(&s.name).ok_or_else(|| {
                    err!(Code::FieldTypeErr, "order by field:{:?} not found", s.name)
                })?;

                let signed = match schema.get_field_entry(schema_field).field_type() {
                    FieldType::I64(_) | FieldType::Date(_) => true,
                    _ => false,
                };

                field_sorts.push(
                    Field::from_field_id(schema_field.field_id() + 1),
                    s.order.eq_ignore_ascii_case("asc"),
                    signed,
                );
            }
            Some(collectors.add_collector(TopDocs::with_limit(size).custom_score(field_sorts)))
        } else {
            None
        };

        let score_top_docs_handle = if !sort_len {
            Some(collectors.add_collector(TopDocs::with_limit(size)))
        } else {
            None
        };

        let count_handle = collectors.add_collector(Count);

        let search_start = SystemTime::now();
        let mut multi_fruit = conver(searcher.search(&q, &collectors))?;

        let count = count_handle.extract(&mut multi_fruit);

        let mut sdr = SearchDocumentResponse {
            code: Code::Success as i32,
            total: count as u64,
            hits: Vec::with_capacity(size),
            info: None, //if this is none means it is success
        };

        if let Some(top_docs_handle) = sort_top_docs_handle {
            let top_docs = top_docs_handle.extract(&mut multi_fruit);

            for (score, doc_address) in top_docs {
                let bytes_reader = searcher
                    .segment_reader(doc_address.0)
                    .fast_fields()
                    .bytes(Field::from_field_id(ID_BYTES_INDEX))
                    .unwrap();
                let doc = bytes_reader.get_bytes(doc_address.1);
                sdr.hits.push(Hit {
                    collection_name: self.collection.name.to_string(),
                    score: 1.0,
                    doc: doc.to_vec(),
                    sort: score.fields,
                });
            }
        } else {
            let top_docs = score_top_docs_handle.unwrap().extract(&mut multi_fruit);

            for (score, doc_address) in top_docs {
                let bytes_reader = searcher
                    .segment_reader(doc_address.0)
                    .fast_fields()
                    .bytes(Field::from_field_id(ID_BYTES_INDEX))
                    .unwrap();
                let doc = bytes_reader.get_bytes(doc_address.1);
                sdr.hits.push(Hit {
                    collection_name: self.collection.name.to_string(),
                    score: score,
                    doc: doc.to_vec(),
                    sort: vec![f32_slice(score)[..].to_vec()],
                });
            }
        }

        let search_finish = SystemTime::now();
        debug!(
            "search: merge result: cost({:?}ms)",
            search_finish
                .duration_since(search_start)
                .unwrap()
                .as_millis()
        );

        Ok(sdr)
    }

    pub fn exist(&self, iid: u32) -> ASResult<bool> {
        let searcher = self.index_reader.searcher();
        let query = TermQuery::new(
            Term::from_field_i64(Field::from_field_id(ID_INDEX), iid as i64),
            IndexRecordOption::Basic,
        );
        let td = TopDocs::with_limit(1);
        let result = conver(searcher.search(&query, &td))?;
        return Ok(result.len() > 0);
    }

    pub fn start_job(index: Arc<Tantivy>, receiver: Receiver<Event>) {
        std::thread::spawn(move || {
            let (cid, pid) = (index.base.collection.id, index.base.partition.id);
            Tantivy::index_job(index, receiver);
            warn!("collection:{}  partition:{} stop index job ", cid, pid);
        });
    }

    pub fn index_job(index: Arc<Tantivy>, rx: Receiver<Event>) {
        loop {
            let e = rx.recv();

            if e.is_err() {
                error!("revice err form index channel:{:?}", e.err());
                if index.base.runing() {
                    continue;
                } else {
                    return;
                }
            }

            let (old_iid, iid) = match e.unwrap() {
                Event::Delete(iid) => (iid, 0),
                Event::Update(old_iid, iid) => (old_iid, iid),
                Event::Stop => {
                    warn!("reviced stop event to stod index loop");
                    return;
                }
            };

            if iid == 0 {
                if old_iid > 0 {
                    if let Err(e) = index._delete(old_iid) {
                        error!("delete:{}  has err:{:?}", old_iid, e);
                    }
                }
            } else {
                match index.db.get_doc_by_id(iid_coding(iid)) {
                    Ok(v) => match v {
                        Some(v) => {
                            if let Err(e) = index._create(old_iid, iid, v) {
                                error!("index values has err:{:?}", e);
                            }
                        }
                        None => error!("not found doc by id:{}", iid),
                    },
                    Err(e) => {
                        error!("index get doc by db has err:{:?}", e);
                    }
                }
            }

            // set status to zero flush will check this value
            index.status.store(0, SeqCst);
        }
    }

    pub fn write(&self, event: Event) -> ASResult<()> {
        conver(self.tx.lock().unwrap().send(event))
    }

    fn _delete(&self, iid: u32) -> ASResult<()> {
        self.check_index()?;
        let ops = self
            .index_writer
            .read()
            .unwrap()
            .delete_term(Term::from_field_i64(Field::from_field_id(0), iid as i64));

        debug!("delete id:{} result:{:?}", iid, ops);
        Ok(())
    }

    fn _create(&self, old_iid: u32, iid: u32, value: Vec<u8>) -> ASResult<()> {
        self.check_index()?;
        let pbdoc: crate::pserverpb::Document =
            prost::Message::decode(prost::bytes::Bytes::from(value))?;

        let mut doc = Document::default();

        doc.add_i64(Field::from_field_id(ID_INDEX), iid as i64);
        doc.add_bytes(
            Field::from_field_id(ID_BYTES_INDEX),
            iid_coding(iid).to_vec(),
        );

        let source: serde_json::Value = serde_json::from_slice(pbdoc.source.as_slice())?;

        let mut flag: bool = false;

        for index in self.collection.scalar_field_index.iter() {
            let field = &self.collection.fields[*index];

            let v = &source[field.name()];
            if v.is_null() {
                if field.none() {
                    continue;
                }
                return result!(Code::ParamError, "field:{} can not be none", field.name());
            }
            let schema = self.index.schema();
            let field_index = schema.get_field(field.name()).unwrap();

            let is_value = field.value();

            if field.array() {
                let values = v.as_array().unwrap();
                for a in values {
                    let v = match field {
                        string(_) | text(_) => Value::Str(json(a)?.try_into()?),
                        int(_) => Value::I64(json(a)?.try_into()?),
                        date(_) => {
                            let naive: NaiveDateTime = json(a)?.try_into()?;
                            Value::Date(DateTime::from_utc(naive, Utc))
                        }
                        float(_) => Value::F64(json(a)?.try_into()?),
                        _ => {
                            return result!(
                                Code::FieldTypeErr,
                                "not support this type :{:?}",
                                field,
                            )
                        }
                    };

                    doc.add(FieldValue::new(field_index, v));
                }

                if is_value {
                    match field {
                        string(_) | text(_) => {
                            let mut strs = vec![];
                            for s in values {
                                strs.push(json(s)?.try_into()?);
                            }
                            doc.add_bytes(
                                Field::from_field_id(field_index.field_id() + 1),
                                str_arr_coding(&strs),
                            );
                        }

                        int(_) => {
                            let mut is = vec![];
                            for s in values {
                                is.push(json(s)?.try_into()?);
                            }
                            doc.add_bytes(
                                Field::from_field_id(field_index.field_id() + 1),
                                i64_arr_coding(&is),
                            );
                        }

                        float(_) => {
                            let mut fs = vec![];
                            for s in values {
                                fs.push(json(s)?.try_into()?);
                            }
                            doc.add_bytes(
                                Field::from_field_id(field_index.field_id() + 1),
                                f64_arr_coding(&fs),
                            );
                        }
                        _ => {
                            return result!(
                                Code::FieldTypeErr,
                                "not support value by this type :{:?}",
                                field,
                            )
                        }
                    }
                }
            } else {
                let v = match field {
                    string(_) | text(_) => {
                        let str: String = json(v)?.try_into()?;
                        if is_value {
                            doc.add_bytes(
                                Field::from_field_id(field_index.field_id() + 1),
                                str.as_bytes().to_vec(),
                            );
                        }
                        Value::Str(str)
                    }
                    int(_) => {
                        let value: i64 = json(v)?.try_into()?;
                        if is_value {
                            doc.add_bytes(
                                Field::from_field_id(field_index.field_id() + 1),
                                value.to_be_bytes().to_vec(),
                            );
                        }
                        Value::I64(value)
                    }
                    date(_) => {
                        let naive: NaiveDateTime = json(v)?.try_into()?;
                        if is_value {
                            doc.add_bytes(
                                Field::from_field_id(field_index.field_id() + 1),
                                naive.timestamp_millis().to_be_bytes().to_vec(),
                            );
                        }
                        Value::Date(DateTime::from_utc(naive, Utc))
                    }
                    float(_) => {
                        let value: f64 = json(v)?.try_into()?;
                        if is_value {
                            doc.add_bytes(
                                Field::from_field_id(field_index.field_id() + 1),
                                value.to_be_bytes().to_vec(),
                            );
                        }
                        Value::F64(value)
                    }
                    _ => return result!(Code::FieldTypeErr, "not support this type :{:?}", field,),
                };

                doc.add(FieldValue::new(field_index, v));
            }

            flag = true;
        }
        let writer = self.index_writer.write().unwrap();
        if old_iid > 0 {
            writer.delete_term(Term::from_field_i64(
                Field::from_field_id(ID_INDEX),
                old_iid as i64,
            ));
        }
        if flag {
            writer.add_document(doc);
        }

        Ok(())
    }

    pub fn check_index(&self) -> ASResult<()> {
        if self.field_num <= 2 {
            return result!(Code::SpaceNoIndex, "space no index");
        }
        Ok(())
    }
}

impl Engine for Tantivy {
    fn flush(&self) -> ASResult<()> {
        if self.status.fetch_add(1, SeqCst) > 10 {
            return Ok(());
        }
        conver(self.index_writer.write().unwrap().commit())?;
        Ok(())
    }

    fn release(&self) {
        info!(
            "the collection:{} , partition:{} to release",
            self.partition.collection_id, self.partition.id
        );
        if let Err(e) = self.flush() {
            error!("flush engine has err:{:?}", e);
        }
    }
}

#[test]
fn test_analyzer() {
    use tantivy::tokenizer::*;
    let tokenizer = TextAnalyzer::from(NgramTokenizer::new(1, 1, false));
    let mut stream = tokenizer.token_stream("hello there 我爱北京天安门");

    while let Some(v) = stream.next() {
        println!("{:?}", v);
    }
}

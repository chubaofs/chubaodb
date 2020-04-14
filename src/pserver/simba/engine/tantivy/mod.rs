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
pub mod bitmap_collector;

use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::pserver::simba::engine::rocksdb::RocksDB;
use crate::pserverpb::*;
use crate::util::coding::iid_coding;
use crate::util::error::*;
use log::{debug, error, info, warn};
use roaring::RoaringBitmap;
use std::fs;
use std::ops::Deref;
use std::path::Path;
use std::sync::{
    mpsc::{channel, Receiver, Sender},
    Arc, Mutex, RwLock,
};
use std::time::SystemTime;
use tantivy::{
    collector::{Count, MultiCollector, TopDocs},
    directory::MmapDirectory,
    query::{QueryParser, TermQuery},
    schema,
    schema::{Field, FieldType as TantivyFT, FieldValue, IndexRecordOption, Schema, Value},
    Document, Index, IndexReader, IndexWriter, ReloadPolicy, Term,
};

const INDEXER_MEMORY_SIZE: usize = 1_000_000_000;
const INDEXER_THREAD: usize = 1;
const ID: &'static str = "_id";
const ID_BYTES: &'static str = "_id_bytes";
const ID_INDEX: u32 = 0;
const ID_BYTES_INDEX: u32 = 1;
const INDEX_DIR_NAME: &'static str = "index";

pub enum Event {
    Delete(i64),
    // Update(old_iid , new_iid)
    Update(i64, i64),
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
        schema_builder.add_text_field(ID, schema::STRING.set_stored());
        schema_builder.add_bytes_field(ID_BYTES);

        for field in base.collection.fields.iter() {
            match field.internal_type {
                crate::util::entity::FieldType::INTEGER => {
                    schema_builder.add_i64_field(
                        field.name.as_ref().unwrap(),
                        schema::IntOptions::default().set_indexed(),
                    );
                }
                crate::util::entity::FieldType::STRING => {
                    schema_builder.add_text_field(field.name.as_ref().unwrap(), schema::STRING);
                }
                crate::util::entity::FieldType::DOUBLE => {
                    schema_builder.add_f64_field(
                        field.name.as_ref().unwrap(),
                        schema::IntOptions::default().set_indexed(),
                    );
                }
                crate::util::entity::FieldType::TEXT => {
                    schema_builder.add_text_field(field.name.as_ref().unwrap(), schema::TEXT);
                }
                crate::util::entity::FieldType::VECTOR => {}
                _ => {
                    return Err(err_box(format!(
                        "thie type:[{}] can not make index",
                        field.field_type.as_ref().unwrap()
                    )))
                }
            }
        }

        let schema = schema_builder.build();
        let field_num = schema.fields().count();

        let index_dir = base.base_path().join(Path::new(INDEX_DIR_NAME));
        if !index_dir.exists() {
            fs::create_dir_all(&index_dir)?;
        }

        let index = convert(Index::open_or_create::<MmapDirectory>(
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

        let tantivy = Arc::new(Tantivy {
            base: base,
            index: index,
            index_writer: RwLock::new(index_writer),
            index_reader: index_reader,
            field_num: field_num,
            db: db,
            tx: Mutex::new(tx),
        });

        Tantivy::start_job(tantivy.clone(), rx);

        info!(
            "init index by collection:{} partition:{} success , use time:{:?} ",
            tantivy.collection.id.unwrap(),
            tantivy.partition.id,
            SystemTime::now().duration_since(now).unwrap().as_millis(),
        );

        Ok(tantivy)
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

    pub fn filter(&self, sdr: Arc<SearchDocumentRequest>) -> ASResult<RoaringBitmap> {
        self.check_index()?;
        let searcher = self.index_reader.searcher();
        let query_parser = QueryParser::for_index(
            &self.index,
            sdr.def_fields
                .iter()
                .map(|s| self.index.schema().get_field(s).unwrap())
                .collect(),
        );
        let q = convert(query_parser.parse_query(sdr.query.as_str()))?;
        let result = convert(searcher.search(&q, &bitmap_collector::Bitmap))?;
        Ok(result)
    }

    pub fn query(&self, sdr: Arc<SearchDocumentRequest>) -> ASResult<SearchDocumentResponse> {
        self.check_index()?;
        let searcher = self.index_reader.searcher();
        let query_parser = QueryParser::for_index(
            &self.index,
            sdr.def_fields
                .iter()
                .map(|s| self.index.schema().get_field(s).unwrap())
                .collect(),
        );
        let size = sdr.size as usize;
        let q = convert(query_parser.parse_query(sdr.query.as_str()))?;

        let mut collectors = MultiCollector::new();
        let top_docs_handle = collectors.add_collector(TopDocs::with_limit(size));
        let count_handle = collectors.add_collector(Count);

        let search_start = SystemTime::now();
        let mut multi_fruit = convert(searcher.search(&q, &collectors))?;

        let count = count_handle.extract(&mut multi_fruit);
        let top_docs = top_docs_handle.extract(&mut multi_fruit);
        let mut sdr = SearchDocumentResponse {
            code: SUCCESS as i32,
            total: count as u64,
            hits: Vec::with_capacity(size),
            info: None, //if this is none means it is success
        };

        for (score, doc_address) in top_docs {
            let bytes_reader = searcher
                .segment_reader(doc_address.0)
                .fast_fields()
                .bytes(Field::from_field_id(ID_BYTES_INDEX))
                .unwrap();

            let doc = bytes_reader.get_bytes(doc_address.1);
            sdr.hits.push(Hit {
                collection_name: self.collection.get_name().to_string(),
                score: score,
                doc: doc.to_vec(),
            });
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

    pub fn exist(&self, iid: i64) -> ASResult<bool> {
        let searcher = self.index_reader.searcher();
        let query = TermQuery::new(
            Term::from_field_i64(Field::from_field_id(ID_INDEX), iid),
            IndexRecordOption::Basic,
        );
        let td = TopDocs::with_limit(1);
        let result = convert(searcher.search(&query, &td))?;
        return Ok(result.len() > 0);
    }

    pub fn start_job(index: Arc<Tantivy>, receiver: Receiver<Event>) {
        std::thread::spawn(move || {
            let (cid, pid) = (index.base.collection.id.unwrap(), index.base.partition.id);
            Tantivy::index_job(index, receiver);
            warn!("collection:{}  partition:{} stop index job ", cid, pid);
        });
    }

    pub fn index_job(index: Arc<Tantivy>, rx: Receiver<Event>) {
        loop {
            let e = rx.recv_timeout(std::time::Duration::from_secs(3));

            if e.is_err() {
                if index.base.runing() {
                    continue;
                } else {
                    return;
                }
            }

            let (old_iid, iid) = match e.unwrap() {
                Event::Delete(iid) => (iid, 0),
                Event::Update(old_iid, iid) => (old_iid, iid),
                Event::Stop => return,
            };

            if iid == 0 {
                if old_iid > 0 {
                    if let Err(e) = index._delete(old_iid) {
                        error!("delete:{}  has err:{:?}", old_iid, e);
                    }
                }
            } else if iid > 0 {
                match index.db.db.get(iid_coding(iid)) {
                    Ok(v) => {
                        if let Some(v) = v {
                            if let Err(e) = index._create(old_iid, iid, v) {
                                error!("index values has err:{:?}", e);
                            }
                        } else {
                            error!("index get doc by db not found");
                        }
                    }
                    Err(e) => {
                        error!("index get doc by db has err:{:?}", e);
                    }
                }
            }
        }
    }

    pub fn write(&self, event: Event) -> ASResult<()> {
        convert(self.tx.lock().unwrap().send(event))
    }

    fn _delete(&self, iid: i64) -> ASResult<()> {
        self.check_index()?;
        let ops = self
            .index_writer
            .read()
            .unwrap()
            .delete_term(Term::from_field_i64(Field::from_field_id(0), iid));

        debug!("delete id:{} result:{:?}", iid, ops);
        Ok(())
    }

    fn _create(&self, old_iid: i64, iid: i64, value: Vec<u8>) -> ASResult<()> {
        self.check_index()?;
        let pbdoc: crate::pserverpb::Document =
            prost::Message::decode(prost::bytes::Bytes::from(value))?;

        let mut doc = Document::default();

        doc.add_i64(Field::from_field_id(ID_INDEX), iid);
        doc.add_bytes(
            Field::from_field_id(ID_BYTES_INDEX),
            iid_coding(iid).to_vec(),
        );

        let source: serde_json::Value = serde_json::from_slice(pbdoc.source.as_slice())?;

        let mut flag: bool = false;

        for (f, fe) in self.index.schema().fields() {
            let v = &source[fe.name()];
            if v.is_null() {
                continue;
            }

            let v = match fe.field_type() {
                &TantivyFT::Str(_) => Value::Str(v.as_str().unwrap().to_string()),
                &TantivyFT::I64(_) => Value::I64(v.as_i64().unwrap()),
                &TantivyFT::F64(_) => Value::F64(v.as_f64().unwrap()),
                _ => {
                    return Err(err_code_box(
                        FIELD_TYPE_ERR,
                        format!("not support this type :{:?}", fe.field_type()),
                    ))
                }
            };
            doc.add(FieldValue::new(f, v));
            flag = true;
        }

        let writer = self.index_writer.write().unwrap();
        if old_iid > 0 {
            writer.delete_term(Term::from_field_i64(Field::from_field_id(0), iid));
        }
        if flag {
            writer.add_document(doc);
        }

        Ok(())
    }

    pub fn check_index(&self) -> ASResult<()> {
        if self.field_num <= 2 {
            return Err(err_code_str_box(PARTITION_NO_INDEX, "partition no index"));
        }
        Ok(())
    }
}

impl Engine for Tantivy {
    fn flush(&self) -> ASResult<()> {
        convert(self.index_writer.write().unwrap().commit())?;
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

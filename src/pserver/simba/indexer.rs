// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.

use crate::pserverpb::*;
use crate::util::{config, entity::*, error::*};
use crate::pserver::simba::simba::Engine ;
use log::{debug, error, info, warn};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc, RwLock};
use std::time::SystemTime;
use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::QueryParser,
    schema,
    schema::{Field, FieldEntry, FieldType as TantivyFT, FieldValue, Schema, Value},
    Document, Index, IndexReader, IndexWriter, ReloadPolicy, Term,
};

const INDEXER_MEMORY_SIZE: usize = 1_000_000_000;
const INDEXER_THREAD: usize = 1;
const ID: &'static str = "_id";
const ID_INDEX: u32 = 0;
const SOURCE: &'static str = "_source";
const SOURCE_INDEX: u32 = 1;
const INDEX_DIR_NAME: &'static str = "index";

pub struct Indexer {
    collection_id: u32,
    collection_name: String,
    partition_id: u32,
    _conf: Arc<config::Config>,
    index: Arc<Index>,
    index_writer: Arc<RwLock<IndexWriter>>,
    index_reader: Arc<IndexReader>,
    field_num: usize,
    stoped: AtomicBool,
}

impl Indexer {
    pub fn new(
        conf: Arc<config::Config>,
        base_path: PathBuf,
        col: &Collection,
        partition: &Partition,
    ) -> ASResult<Arc<Indexer>> {
        let now = SystemTime::now();

        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field(ID, schema::STRING.set_stored());
        schema_builder.add_bytes_field(SOURCE);

        for field in col.fields.iter().filter(|f| f.index.unwrap()) {
            match field.internal_type.as_ref().unwrap() {
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

        let index_dir = base_path.join(Path::new(INDEX_DIR_NAME));
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

        let indexer = Arc::new(Indexer {
            collection_id: partition.collection_id,
            collection_name: col.get_name().to_string(),
            partition_id: partition.id,
            _conf: conf,
            index: Arc::new(index),
            index_writer: Arc::new(RwLock::new(index_writer)),
            index_reader: Arc::new(index_reader),
            field_num: field_num,
            stoped: AtomicBool::new(false),
        });

        info!(
            "init index by collection:{} partition:{} success , use time:{:?} ",
            indexer.collection_id,
            indexer.partition_id,
            SystemTime::now().duration_since(now).unwrap().as_millis(),
        );

        Ok(indexer)
    }

    pub fn release(&self) {
        self.stoped.store(true, std::sync::atomic::Ordering::SeqCst);
        warn!("partition:{} index released", self.partition_id);
    }

    pub fn search(&self, sdr: Arc<SearchDocumentRequest>) -> ASResult<SearchDocumentResponse> {
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
        let limit = TopDocs::with_limit(size);

        let search_start = SystemTime::now();
        let top_docs = convert(searcher.search(&q, &limit))?;
        let mut sdr = SearchDocumentResponse {
            code: SUCCESS as i32,
            total: 0,
            hits: Vec::with_capacity(size),
            info: None, //if this is none means it is success
        };

        for (score, doc_address) in top_docs {
            let retrieved_doc = convert(searcher.doc(doc_address))?;
            if let Value::Bytes(doc) = retrieved_doc
                .get_first(Field::from_field_id(SOURCE_INDEX))
                .unwrap()
            {
                sdr.hits.push(Hit {
                    collection_name: self.collection_name.clone(),
                    score: score,
                    doc: doc.to_vec(),
                });
            } else {
                error!(
                    "source not found by value :{:?}",
                    retrieved_doc
                        .get_first(Field::from_field_id(SOURCE_INDEX))
                        .unwrap()
                );
            };
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

    pub fn write(&self, key: &Vec<u8>, value: &Vec<u8>) -> ASResult<()> {
        let iid = base64::encode(key);

        let pbdoc: crate::pserverpb::Document =
            prost::Message::decode(prost::bytes::Bytes::from(value.to_vec()))?;

        let source: serde_json::Value = serde_json::from_slice(pbdoc.source.as_slice())?;

        let mut doc = Document::default();
        doc.add_text(Field::from_field_id(ID_INDEX), iid.as_str());
        doc.add_bytes(Field::from_field_id(SOURCE_INDEX), value.clone());
        let schema = self.index.schema();
        for (k, v) in source.as_object().unwrap() {
            if let Some(f) = schema.get_field(k) {
                let entry: &FieldEntry = schema.get_field_entry(f);

                let v = match entry.field_type() {
                    &TantivyFT::Str(_) => Value::Str(v.as_str().unwrap().to_string()),
                    &TantivyFT::I64(_) => Value::I64(v.as_i64().unwrap()),
                    &TantivyFT::F64(_) => Value::F64(v.as_f64().unwrap()),
                    _ => {
                        return Err(err_code_box(
                            FIELD_TYPE_ERR,
                            format!("not support this type :{:?}", entry.field_type()),
                        ))
                    }
                };
                doc.add(FieldValue::new(f, v));
            };
        }

        self.index_writer
            .read()
            .unwrap()
            .delete_term(Term::from_field_text(Field::from_field_id(0), iid.as_str()));
        self.index_writer.read().unwrap().add_document(doc);

        Ok(())
    }

    pub fn delete(&self, key: &Vec<u8>) -> ASResult<()> {
        let iid = base64::encode(key);
        self.index_writer
            .read()
            .unwrap()
            .delete_term(Term::from_field_text(Field::from_field_id(0), iid.as_str()));
        Ok(())
    }

    pub fn check_index(&self) -> ASResult<()> {
        if self.field_num <= 1 {
            return Err(err_code_str_box(PARTITION_NO_INDEX, "partition no index"));
        }
        Ok(())
    }
}

impl Engine for Indexer {
    fn            get_sn(&self) -> u64{
        panic!()
    }
     fn set_sn_if_max(&self, sn: u64){
        panic!()
    }
     fn flush(&self) -> ASResult<()>{
        panic!()
    }
     fn release(&self) {
        panic!()
    }
}

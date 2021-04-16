use crate::pserver::simba::aggregation::Aggregator;
use std::sync::{Arc, RwLock};
use tantivy::{
    collector::Collector, collector::SegmentCollector, fastfield::BytesFastFieldReader,
    schema::Field, DocId, Score, SegmentLocalId, SegmentReader,
};

#[derive(Default)]
pub struct Aggregation {
    linker: String,
    agg: Arc<RwLock<Aggregator>>,
    group_fields: Vec<Field>,
    fun_fields: Vec<Field>,
}

impl Aggregation {
    pub fn new(
        linker: &str,
        agg: Arc<RwLock<Aggregator>>,
        group_fields: Vec<Field>,
        fun_fields: Vec<Field>,
    ) -> Aggregation {
        Self {
            linker: linker.to_string(),
            agg,
            group_fields,
            fun_fields,
        }
    }
}

impl Collector for Aggregation {
    type Fruit = ();

    type Child = SegmentAggregationCollector;

    fn for_segment(
        &self,
        _: SegmentLocalId,
        sr: &SegmentReader,
    ) -> tantivy::Result<SegmentAggregationCollector> {
        Ok(SegmentAggregationCollector {
            linker: self.linker.clone(),
            agg: self.agg.clone(),
            groups: self
                .group_fields
                .iter()
                .map(|f| sr.fast_fields().bytes(*f).unwrap())
                .collect(),
            funs: self
                .fun_fields
                .iter()
                .map(|f| sr.fast_fields().bytes(*f).unwrap())
                .collect(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, _: Vec<()>) -> tantivy::Result<()> {
        Ok(())
    }
}

pub struct SegmentAggregationCollector {
    linker: String,
    agg: Arc<RwLock<Aggregator>>,
    groups: Vec<BytesFastFieldReader>,
    funs: Vec<BytesFastFieldReader>,
}

impl SegmentCollector for SegmentAggregationCollector {
    type Fruit = ();

    fn collect(&mut self, doc_id: DocId, _: Score) {
        let mut values = Vec::with_capacity(self.funs.len());
        for r in self.funs.iter() {
            values.push(r.get_bytes(doc_id));
        }
        self._collect(Vec::with_capacity(self.groups.len()), doc_id, 0, &values);
    }

    fn harvest(self) -> Self::Fruit {
        ()
    }
}

impl SegmentAggregationCollector {
    fn _collect(&self, mut path: Vec<String>, doc_id: DocId, index: usize, values: &Vec<&[u8]>) {
        if index >= self.groups.len() {
            let path = path.join(self.linker.as_str());
            if let Err(e) = self.agg.write().unwrap().map(path.as_str(), values) {
                tracing::log::error!("{:?}", e);
            };
            return;
        };

        //TODO: if it panic???
        let keys = self.agg.read().unwrap().groups[index]
            .coding(self.groups[index].get_bytes(doc_id))
            .unwrap();

        if keys.len() == 0 {
            return;
        }

        if keys.len() == 1 {
            path.push(keys.into_iter().next().unwrap());
            self._collect(path, doc_id, index + 1, values);
        } else {
            for key in keys {
                let mut path = path.clone();
                path.push(key);
                self._collect(path, doc_id, index + 1, values);
            }
        }
    }
}

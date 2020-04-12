use crate::pserver::simba::engine::tantivy::ID_BYTES_INDEX;
use roaring::RoaringBitmap;
use std::sync::Mutex;
use tantivy::{
    collector::SegmentCollector, schema::Field, BytesFastFieldReader, Collector, DocId, Score,
    SegmentLocalId, SegmentReader,
};
pub struct Bitmap {
    bit_map: RoaringBitmap,
}

impl Collector for Count {
    type Fruit = RoaringBitmap;

    type Child = SegmentCountCollector;

    fn for_segment(
        &self,
        sl: SegmentLocalId,
        sr: &SegmentReader,
    ) -> crate::Result<SegmentCountCollector> {
        sr.fast_fields().bytes(Field::from_field_id(ID_BYTES_INDEX));
        Ok(SegmentCountCollector::default())
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_counts: Vec<usize>) -> crate::Result<usize> {
        Ok(segment_counts.into_iter().sum())
    }
}

pub struct SegmentBitMapCollector<'a> {
    bit_map: &'a Mutex<RoaringBitmap>,
    reader: BytesFastFieldReader,
}

impl SegmentCollector for SegmentBitMapCollector {
    type Fruit = RoaringBitmap;

    fn collect(&mut self, doc_id: DocId, _: Score) {
        if Some(r) = self.as_ref() {
            r.get_bytes(doc_id);
        }
    }

    fn harvest(self) -> usize {
        self.count
    }
}

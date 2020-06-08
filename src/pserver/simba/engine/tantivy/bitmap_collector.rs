use crate::pserver::simba::engine::tantivy::ID_BYTES_INDEX;
use crate::util::coding::slice_i64;
use roaring::RoaringBitmap;
use tantivy::{
    collector::Collector, collector::SegmentCollector, fastfield::BytesFastFieldReader,
    schema::Field, DocId, Score, SegmentLocalId, SegmentReader,
};
#[derive(Default)]
pub struct Bitmap;

impl Collector for Bitmap {
    type Fruit = RoaringBitmap;

    type Child = SegmentBitMapCollector;

    fn for_segment(
        &self,
        _: SegmentLocalId,
        sr: &SegmentReader,
    ) -> tantivy::Result<SegmentBitMapCollector> {
        Ok(SegmentBitMapCollector {
            bit_map: RoaringBitmap::new(),
            fast_field: sr.fast_fields().bytes(Field::from_field_id(ID_BYTES_INDEX)),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_bitmaps: Vec<RoaringBitmap>) -> tantivy::Result<RoaringBitmap> {
        let result = segment_bitmaps
            .iter()
            .fold(RoaringBitmap::default(), |r, v| r | v);

        return Ok(result);
    }
}

pub struct SegmentBitMapCollector {
    bit_map: RoaringBitmap,
    fast_field: Option<BytesFastFieldReader>,
}

impl SegmentCollector for SegmentBitMapCollector {
    type Fruit = RoaringBitmap;

    fn collect(&mut self, doc_id: DocId, _: Score) {
        if let Some(ffr) = self.fast_field.as_ref() {
            let v = ffr.get_bytes(doc_id);
            if v.len() > 0 {
                self.bit_map.insert(slice_i64(v) as u32);
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.bit_map
    }
}

use crate::util::coding::sort_coding;
use std::cmp::Ordering;
use tantivy::{
    collector::{CustomScorer, CustomSegmentScorer},
    fastfield::BytesFastFieldReader,
    schema::Field,
    DocId, SegmentReader,
};

#[derive(Clone)]
pub struct FieldScore {
    pub fields: Vec<Vec<u8>>,
    asc: Vec<bool>,
}

impl FieldScore {
    fn new(cap: usize) -> FieldScore {
        FieldScore {
            fields: Vec::with_capacity(cap),
            asc: Vec::with_capacity(cap),
        }
    }

    fn push(&mut self, v: Vec<u8>, asc: bool) {
        self.fields.push(v);
        self.asc.push(asc);
    }

    pub fn cmp_by_order(s: &Vec<Vec<u8>>, o: &Vec<Vec<u8>>, asc: &Vec<bool>) -> Ordering {
        let len = asc.len();
        for i in 0..len {
            let asc = asc[i];
            let order = Self::cmp(&s[i], &o[i]);
            if order != Ordering::Equal {
                if asc {
                    return order;
                } else {
                    return order.reverse();
                }
            }
        }
        Ordering::Equal
    }

    fn cmp(s: &[u8], o: &[u8]) -> Ordering {
        //default num compare
        if s[0] != crate::util::coding::sort_coding::STR {
            return s.cmp(o);
        }

        let mut s = sort_coding::str_arr_decoding(s);
        let mut o = sort_coding::str_arr_decoding(o);

        loop {
            let s = s.next();
            let o = o.next();

            if s.is_none() && o.is_none() {
                return Ordering::Equal;
            } else if o.is_none() {
                return Ordering::Greater;
            } else if s.is_none() {
                return Ordering::Less;
            }

            let order = s.unwrap().cmp(o.unwrap());

            if order != Ordering::Equal {
                return order;
            }
        }
    }
}

impl PartialOrd for FieldScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        for (i, v) in other.fields.iter().enumerate() {
            let asc = self.asc[i];
            let order = FieldScore::cmp(&self.fields[i], v);
            if order != Ordering::Equal {
                if asc {
                    return Some(order);
                } else {
                    return Some(order.reverse());
                }
            }
        }
        return Some(Ordering::Equal);
    }
}

impl PartialEq for FieldScore {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

// order asc is true , desc is false
#[derive(Clone)]
struct FieldSort {
    field: Field,
    asc: bool,
    signed: bool,
}

pub struct FieldSorts(Vec<FieldSort>);

impl FieldSorts {
    pub fn new(cap: usize) -> FieldSorts {
        FieldSorts(Vec::with_capacity(cap))
    }

    pub fn push(&mut self, field: Field, asc: bool, signed: bool) {
        self.0.push(FieldSort { field, asc, signed });
    }
}

impl CustomScorer<FieldScore> for FieldSorts {
    type Child = FieldFastReader;

    fn segment_scorer(&self, segment_reader: &SegmentReader) -> tantivy::Result<Self::Child> {
        let mut ffr = FieldFastReader::with_capacity(self.0.len());

        for fs in self.0.iter() {
            let reader = segment_reader
                .fast_fields()
                .bytes(fs.field)
                .ok_or_else(|| {
                    tantivy::TantivyError::SchemaError(format!(
                        "Field requested ({:?}) is not a i64/u64 fast field.",
                        fs.field
                    ))
                })?;

            ffr.push(FieldReader {
                reader,
                asc: fs.asc,
                signed: fs.signed,
            });
        }

        Ok(ffr)
    }
}

pub struct FieldReader {
    reader: BytesFastFieldReader,
    asc: bool,
    signed: bool,
}

//the second param is signed , for datetime or i64 sort
type FieldFastReader = Vec<FieldReader>;

impl CustomSegmentScorer<FieldScore> for FieldFastReader {
    fn score(&self, doc: DocId) -> FieldScore {
        let mut fs = FieldScore::new(self.len());
        for fr in self.iter() {
            let mut v = fr.reader.get_bytes(doc).to_vec();
            if fr.signed && v.len() > 0 {
                v[0] = v[0] ^ 128;
            }
            fs.push(v, fr.asc);
        }
        fs
    }
}

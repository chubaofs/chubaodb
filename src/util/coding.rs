use alaya_protocol::pserver::Document;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::slice;

pub fn hash_str(v: &str) -> u64 {
    let mut s = DefaultHasher::new();
    v.hash(&mut s);
    s.finish()
}

pub fn slice_u16(pack_data: &[u8]) -> u16 {
    let mut v: [u8; 2] = Default::default();
    v.copy_from_slice(pack_data);
    fix_slice_u16(v)
}

pub fn slice_i32(pack_data: &[u8]) -> i32 {
    let mut v: [u8; 4] = Default::default();
    v.copy_from_slice(pack_data);
    fix_slice_i32(v)
}

pub fn slice_u32(pack_data: &[u8]) -> u32 {
    let mut v: [u8; 4] = Default::default();
    v.copy_from_slice(pack_data);
    fix_slice_u32(v)
}

pub fn slice_u64(pack_data: &[u8]) -> u64 {
    let mut v: [u8; 8] = Default::default();
    v.copy_from_slice(pack_data);
    fix_slice_u64(v)
}

pub fn slice_i64(pack_data: &[u8]) -> i64 {
    let mut v: [u8; 8] = Default::default();
    v.copy_from_slice(pack_data);
    fix_slice_i64(v)
}

pub fn slice_f32(pack_data: &[u8]) -> f32 {
    let mut v: [u8; 4] = Default::default();
    v.copy_from_slice(pack_data);
    fix_slice_f32(v)
}

pub fn slice_f64(pack_data: &[u8]) -> f64 {
    let mut v: [u8; 8] = Default::default();
    v.copy_from_slice(pack_data);
    fix_slice_f64(v)
}

pub fn fix_slice_u16(v: [u8; 2]) -> u16 {
    u16::from_be_bytes(v)
}

pub fn fix_slice_u32(v: [u8; 4]) -> u32 {
    u32::from_be_bytes(v)
}

pub fn fix_slice_i32(v: [u8; 4]) -> i32 {
    i32::from_be_bytes(v)
}

pub fn fix_slice_u64(v: [u8; 8]) -> u64 {
    u64::from_be_bytes(v)
}

pub fn fix_slice_i64(v: [u8; 8]) -> i64 {
    i64::from_be_bytes(v)
}

pub fn fix_slice_f64(v: [u8; 8]) -> f64 {
    f64::from_be_bytes(v)
}

pub fn fix_slice_f32(v: [u8; 4]) -> f32 {
    f32::from_be_bytes(v)
}

pub fn u32_slice(value: u32) -> [u8; 4] {
    value.to_be_bytes()
}

pub fn i32_slice(value: i32) -> [u8; 4] {
    value.to_be_bytes()
}

pub fn u64_slice(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

pub fn u16_slice(value: u16) -> [u8; 2] {
    value.to_be_bytes()
}

pub fn i64_slice(value: i64) -> [u8; 8] {
    value.to_be_bytes()
}

pub fn f64_slice(value: f64) -> [u8; 8] {
    value.to_be_bytes()
}

pub fn f32_slice(value: f32) -> [u8; 4] {
    value.to_be_bytes()
}

pub fn split_u32(value: u64) -> (u32, u32) {
    let bytes = value.to_be_bytes();

    (slice_u32(&bytes[4..]), slice_u32(&bytes[..4]))
}

pub fn merge_u32(v1: u32, v2: u32) -> u64 {
    let mut v2 = u32_slice(v2).to_vec();
    let v1 = u32_slice(v1);

    v2.extend_from_slice(&v1);

    slice_u64(v2.as_slice())
}

pub fn base64(value: &Vec<u8>) -> String {
    base64::encode(value)
}

/**
 * id coding has two model
 * 0. doc id : u32(iid) = proto(document) //first must 0
 * 1. SN_KEY [1, '_', '_', '_', '_', 's', 'n']
 * 2. doc key: u8(2) + str(id) = field key
 * 3. doc key: u8(3) + hash_str(id) + id + 0 + sort_key = field key
 * 4. field key : u8(4) + str(field_name) + 0 + u32(iid) = field_value
 * if sort_key is "" it use to 1.
 * if sort_key is not "" , it use 2.
 * the id  for routing partition
 *
 */
pub const RAFT_INDEX_KEY: &'static [u8; 2] = &[1, 1];

pub fn key_type(key: &Vec<u8>) -> &'static str {
    match key[0] {
        0 => "doc_value",
        1 => "SN_KEY",
        2 => "doc_id",
        3 => "doc_id",
        4 => "vector",
        5 => "scalar",
        _ => "unknow",
    }
}

pub fn iid_coding(iid: u32) -> [u8; 4] {
    u32_slice(iid)
}

pub fn vector_field_coding(field_name: &str, iid: u32) -> Vec<u8> {
    let mut arr = Vec::with_capacity(6 + field_name.len());
    arr.push(4);
    arr.extend_from_slice(field_name.as_bytes());
    arr.push(0); //here may be has bug
    arr.extend_from_slice(&u32_slice(iid));
    arr
}

pub fn scalar_field_coding(field_id: usize, mut value: Vec<u8>, len: usize, iid: u32) -> Vec<u8> {
    let mut arr = Vec::with_capacity(10 + value.len());
    arr.push(5);
    arr.extend_from_slice(&u32_slice(field_id as u32));
    if len == value.len() {
        arr.extend_from_slice(&value);
    } else if len < value.len() {
        arr.extend_from_slice(&value[..len]);
    } else {
        unsafe {
            value.set_len(len);
        }
        arr.extend_from_slice(&value);
    }
    arr.extend_from_slice(&u32_slice(iid));
    arr
}

pub fn key_coding(id: &str, sort_key: &str) -> Vec<u8> {
    if sort_key.is_empty() {
        let mut arr = Vec::with_capacity(1 + id.len());
        arr.push(2);
        arr.extend_from_slice(id.as_bytes());
        return arr;
    }

    let mut arr = Vec::with_capacity(10 + id.len() + sort_key.len());
    arr.push(3);
    arr.extend(hash_str(id).to_be_bytes().to_vec());
    arr.extend_from_slice(id.as_bytes());
    arr.push(0);
    arr.extend_from_slice(sort_key.as_bytes());

    arr
}

pub fn doc_key(doc: &Document) -> Vec<u8> {
    key_coding(doc.id.as_str(), doc.sort_key.as_str())
}

pub fn slice_slice<'a, T, V>(s: &'a [T]) -> &'a [V] {
    unsafe {
        slice::from_raw_parts(
            s.as_ptr() as *const _,
            s.len() / size_of::<V>() * size_of::<T>(),
        )
    }
}

pub mod sort_coding {

    pub const INT: u8 = 0;
    pub const FLOAT: u8 = 1;
    pub const STR: u8 = 2;

    pub fn str_arr_coding(strs: &Vec<String>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(strs.len() * 16);
        buf.push(STR);
        for s in strs {
            if s.len() > 256 {
                continue;
            }
            buf.push(s.len() as u8);
            if s.len() > 0 {
                buf.extend_from_slice(s.as_bytes());
            }
        }
        buf.to_vec()
    }

    pub struct StrIter<'a> {
        value: &'a [u8],
        offset: usize,
    }

    impl<'a> StrIter<'a> {
        pub fn next(&mut self) -> Option<&'a [u8]> {
            if self.offset >= self.value.len() {
                return None;
            }
            let len = self.value[self.offset] as usize;
            self.offset += 1;
            if self.offset + len > self.value.len() {
                return None;
            }
            self.offset += len;
            Some(&self.value[self.offset - len..self.offset])
        }
    }

    pub fn str_arr_decoding<'a>(arr: &'a [u8]) -> StrIter<'a> {
        StrIter {
            value: arr,
            offset: 1,
        }
    }

    pub fn f64_arr_coding(fs: &Vec<f64>) -> Vec<u8> {
        let mut result = Vec::with_capacity(fs.len() * 8 + 1);
        result.push(FLOAT);
        for f in fs {
            result.extend_from_slice(&f.to_be_bytes()[..]);
        }
        result
    }

    pub fn f64_arr_decoding(arr: &[u8]) -> Vec<f64> {
        let num = (arr.len() - 1) / 4;
        let mut result = Vec::with_capacity(num);
        for i in 0..num {
            result.push(crate::util::coding::slice_f64(&arr[i * 8 + 1..i + 8]));
        }
        result
    }

    pub fn i64_arr_coding(is: &Vec<i64>) -> Vec<u8> {
        let mut result = Vec::with_capacity(is.len() * 8 + 1);
        result.push(INT);
        for i in is {
            result.extend_from_slice(&i.to_be_bytes()[..]);
        }
        result
    }

    pub fn i64_arr_decoding(arr: &[u8]) -> Vec<i64> {
        let num = (arr.len() - 1) / 4;
        let mut result = Vec::with_capacity(num);
        for i in 0..num {
            result.push(crate::util::coding::slice_i64(&arr[i * 8 + 1..i + 8]));
        }
        result
    }
}

#[test]
pub fn test_i64_to_slice() {
    let a = 123;
    println!("{:?}", i64_slice(a));
}

#[test]
pub fn test_slice_slice() {
    let a: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];
    let value: &[u8] = slice_slice(&a);
    println!("{:?}", value);

    let vec = value.to_vec();
    let value: &[f32] = slice_slice(&vec);
    println!("{:?}", value);

    vec.as_slice();

    assert_eq!(a, value);
}

#[test]
pub fn test_coding_u32() {
    let a: u32 = 100;
    let s = u32_slice(a);
    let b = slice_u32(&s);
    println!("slice_u32:{:?}", b);
    let b = fix_slice_u32(s);
    println!("u32_slice:{:?}", s);
    assert_eq!(a, b);
}

#[test]
pub fn test_split_u32() {
    let v: u64 = 2132391239123;
    let (a, b) = split_u32(v);
    let v1 = merge_u32(a, b);
    assert_eq!(v, v1);

    let collection_id = 12;
    let partition_id = 32;

    let value = merge_u32(collection_id, partition_id);

    println!("merge: {}", value);

    let (cid, pid) = split_u32(value);

    println!("partition_id:{}", partition_id);
    println!("collection_id:{}", collection_id);

    assert_eq!(cid, collection_id);
    assert_eq!(pid, partition_id);
}

#[test]
pub fn string_arr_coding_decoding() {
    let arr = vec![
        String::from("123"),
        String::from("123456"),
        String::from("123456789"),
    ];

    let value = sort_coding::str_arr_coding(&arr);
    println!("str array coding: {:?}", value);

    let mut iter = sort_coding::str_arr_decoding(&value);

    for a in arr {
        assert_eq!(a.as_bytes(), iter.next().unwrap());
    }

    assert_eq!(None, iter.next());

    let arr = vec![
        String::from("123"),
        String::from(""),
        String::from("123456"),
        String::from("123456789"),
    ];

    let value = sort_coding::str_arr_coding(&arr);

    let mut iter = sort_coding::str_arr_decoding(&value);

    for a in arr {
        assert_eq!(a.as_bytes(), iter.next().unwrap());
    }

    assert_eq!(None, iter.next());

    let arr = vec![];

    let value = sort_coding::str_arr_coding(&arr);
    println!("str array coding: {:?}", value);

    let mut iter = sort_coding::str_arr_decoding(&value);

    for a in arr {
        assert_eq!(a.as_bytes(), iter.next().unwrap());
    }

    assert_eq!(None, iter.next());
}

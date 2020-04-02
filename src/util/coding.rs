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
use crate::pserverpb::Document;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::slice;

pub fn hash_str(v: &str) -> u64 {
	let mut s = DefaultHasher::new();
	v.hash(&mut s);
	s.finish()
}

//it is not
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

pub fn fix_slice_u32(v: [u8; 4]) -> u32 {
	u32::from_be_bytes(v)
}

pub fn fix_slice_u64(v: [u8; 8]) -> u64 {
	u64::from_be_bytes(v)
}

pub fn fix_slice_i64(v: [u8; 8]) -> i64 {
	i64::from_be_bytes(v)
}

pub fn u32_slice(value: u32) -> [u8; 4] {
	value.to_be_bytes()
}

pub fn u64_slice(value: u64) -> [u8; 8] {
	value.to_be_bytes()
}

pub fn i64_slice(value: i64) -> [u8; 8] {
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
 * 0. doc id : i64(iid) = proto(document) //first must 0
 * 1. SN_KEY [1, '_', '_', '_', '_', 's', 'n']
 * 2. doc key: u8(2) + str(id) = field key
 * 3. doc key: u8(3) + hash_str(id) + id + 0 + sort_key = field key
 * 4. field key : u8(4) + str(field_name) + 0 + i64(iid) = field_value
 * if sort_key is "" it use to 1.
 * if sort_key is not "" , it use 2.
 * the id  for routing partition
 *
 */

pub const SN_KEY: &'static [u8; 1] = &[1];

pub fn iid_coding(iid: i64) -> [u8; 8] {
	i64_slice(iid)
}

pub fn field_coding(field_name: &str, iid: i64) -> Vec<u8> {
	let mut arr = Vec::with_capacity(9 + field_name.len());
	arr.push(4);
	arr.extend_from_slice(field_name.as_bytes());
	arr.push(0);
	arr.extend_from_slice(&i64_slice(iid));
	arr
}

pub fn key_coding(id: &str, sort_key: &str) -> Vec<u8> {
	if sort_key.is_empty() {
		let mut arr = Vec::with_capacity(1 + id.len());
		arr.push(2);
		arr.extend_from_slice(id.as_bytes());
		return arr;
	}

	let mut arr = Vec::with_capacity(6 + id.len() + sort_key.len());
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

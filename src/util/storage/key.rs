use std::io::Write;

use byteorder::{WriteBytesExt, BE};
use smallvec::SmallVec;

use crate::common::bytes_prefix;

const LAST_APPLIED_LOG: u8 = 1;
const HARD_STATE: u8 = 2;
const ENTRY: u8 = 3;

#[inline]
pub fn last_applied_log(scope: &[u8]) -> SmallVec<[u8; 32]> {
    let mut key = SmallVec::default();
    key.write_all(scope).unwrap();
    key.write_u8(LAST_APPLIED_LOG).unwrap();
    key
}

#[inline]
pub fn hard_state(scope: &[u8]) -> SmallVec<[u8; 32]> {
    let mut key = SmallVec::default();
    key.write_all(scope).unwrap();
    key.write_u8(HARD_STATE).unwrap();
    key
}

#[inline]
pub fn entry(scope: &[u8], index: u64) -> SmallVec<[u8; 32]> {
    let mut key = SmallVec::default();
    key.write_all(scope).unwrap();
    key.write_u8(ENTRY).unwrap();
    key.write_u64::<BE>(index).unwrap();
    key
}

#[inline]
pub fn entry_end(scope: &[u8]) -> SmallVec<[u8; 32]> {
    let mut key = SmallVec::default();
    key.write_all(scope).unwrap();
    key.write_u8(ENTRY + 1).unwrap();
    key
}

#[inline]
pub fn is_entry(scope: &[u8], data: &[u8]) -> bool {
    if data.len() != scope.len() + 1 + 8 {
        return false;
    }
    &data[..scope.len()] == scope && data[scope.len()] == ENTRY
}

#[inline]
pub fn data(scope: &[u8], data_key: &[u8]) -> SmallVec<[u8; 32]> {
    let mut key = SmallVec::default();
    key.write_all(scope).unwrap();
    key.write_all(data_key).unwrap();
    key
}

#[inline]
pub fn decode_data<'a>(scope: &[u8], data: &'a [u8]) -> Option<&'a [u8]> {
    if data.len() < scope.len() + 1 {
        return None;
    }
    Some(&data[scope.len() + 1..])
}

#[inline]
pub fn scope_start(scope: &[u8]) -> SmallVec<[u8; 32]> {
    let mut key = SmallVec::default();
    key.write_all(scope).unwrap();
    key
}

#[inline]
pub fn scope_end(scope: &[u8]) -> SmallVec<[u8; 32]> {
    let mut key: SmallVec<[u8; 32]> = Default::default();
    key.write_all(scope).unwrap();
    bytes_prefix(&key).1
}

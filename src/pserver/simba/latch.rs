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
use crate::util::error::*;
use crate::*;
use std::sync::Mutex;
pub struct Latch {
    locks: Vec<Mutex<usize>>,
    size: usize,
}

impl Latch {
    pub fn new(size: usize) -> Latch {
        let mut locks = Vec::with_capacity(size);
        for i in 0..size {
            locks.push(Mutex::new(i));
        }
        Latch {
            locks: locks,
            size: size,
        }
    }
    pub fn latch_lock<'a>(&'a self, slot: u32) -> &'a Mutex<usize> {
        &(self.locks[slot as usize % self.size])
    }

    pub fn latch<T>(&self, slot: u32, f: impl FnOnce() -> ASResult<T>) -> ASResult<T> {
        match self.locks[slot as usize % self.size].lock() {
            Ok(_) => f(),
            Err(e) => result_def!("get latch lock has err:{}", e),
        }
    }
}

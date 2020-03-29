use crate::pserver::simba::engine::engine::{BaseEngine, Engine};
use crate::util::error::*;
use cpp::cpp;
use std::ops::Deref;

cpp! {{
    #include <iostream>

    #include <cmath>
    #include <cstdio>
    #include <cstdlib>

    #include <sys/time.h>


    #include <faiss/IndexPQ.h>
    #include <faiss/IndexIVFFlat.h>
    #include <faiss/IndexFlat.h>
    #include <faiss/index_io.h>

    double elapsed ()
    {
        struct timeval tv;
        gettimeofday (&tv, nullptr);
        return  tv.tv_sec + tv.tv_usec * 1e-6;
    }
}}

pub struct Faiss {
    base: BaseEngine,
}

impl Deref for Faiss {
    type Target = BaseEngine;
    fn deref<'a>(&'a self) -> &'a BaseEngine {
        &self.base
    }
}

impl Faiss {
    pub fn new(base: BaseEngine) -> ASResult<Faiss> {
        unsafe {
            cpp!([]{
                int d = 128;
                size_t nb = 1000 * 1000;
                size_t nt = 100 * 1000;
                size_t nhash = 2;
                size_t nbits_subq = int (log2 (nb+1) / 2);        // good choice in general
                size_t ncentroids = 1 << (nhash * nbits_subq);  // total # of centroids
                faiss::MultiIndexQuantizer coarse_quantizer (d, nhash, nbits_subq);
            });
        }

        Ok(Faiss { base: base })
    }
}

impl Engine for Faiss {
    fn flush(&self) -> ASResult<()> {
        Ok(())
    }

    fn release(&self) {}
}

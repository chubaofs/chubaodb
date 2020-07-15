# chubaodb

ChubaoDB is a cloud native distributed document database on top of ChubaoFS. 

As a scalable non-relational structured data infrastructure, ChubaoDB has several key features:

* flexible data model

* disaggregated storage and compute architecture

* rich indexes for efficient search



## External Interface

collection, document, field

dockey -> document, dockey = (hashkey:string, sortkey:string), hash key (also called partition key) is used for data partitioning, and sort key (not necessary and can be empty) is used for data ordering - all documents with the same hashkey value are stored together, in sorted order by sortkey value.

fields that are defined in the schema are indexed for fast search. 

document API: Create, Update, Upsert, Delete, Overwrite, Get, Search, Count, ...


## Architecture

master, partition server, router

orchestracted by Kubernetes

ChubaoFS (replicated across three AZs) works as the underlying storage infrastructure


## Licence

Apache 2


## Acknowledgments

* jimraft

* rocksdb

* tantivy



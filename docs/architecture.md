# ChubaoDB internals and design choices

This draft explains the inner workings of ChubaoDB, as well as some key design decisions we made. 

## data model

collection, document, field

recognize the presence of two primary forms of structured data: nested entities and independent entities

dockey = (hashkey:string, sortkey:string), sortkey is optional, dockey -> document

so chubaodb can support documents, sparse big tables, and even graphs.

field data types: integer, float, string, vector, etc; and fields can be indexed.


## distributed architecture

A ChubaoDB cluster is usually deployed to multiple (usually three) availability zones.
Cross-zone replication provides higher availability, higher durability, and resilience in the face of zonal failures. 

master, partition server, router

partitions work as the replication unit, and a PS hosts one or multiple partition replicas.

Partition -> Simba

we leverage Chubaofs as the storage infrastructure to seperate compute from storage for more flexible resource scheduling, and faster failover or partition transfer

directory structure on a CFS instance: 

cluster-id/collections/collection-id/partition-id/datafiles

two options: 
1, one CFS per zone - transparent DISC; 
2, CFS replication across the three zones. here we prefer 2.

## partition replication

firstly we try no compute-layer replication but just rely on CFS storage layer for strong persistence, sync write of rocksdb wal with server-level group commit optimization. 
however, the performance metrics were not good. 


multi-raft, one raft state machine per partition. 
still buffered write of rocksdb wal, and DB.SyncWAL() per say 1000 writes; only the leader serves read/write operations and the followers hold raft commands to catch up on the lost buffered writes in case of leader failure.

## scalability

dynamic re-balancing of partition replica placement

unlimited partition size with CFS

scaling up: bigger containers for partitions with more traffic

## Router

### protocols

GraphQL
gRPC
RESTful

### APIs

get/search
create/update/upsert/delete/overwrite

## Master

cluster management

replication topology graph

dynamic partition rebalancing

### interface

* CreateCollection
* CreatePartition
* AddServer

### data structures

ZoneInfo -> ServerInfo -> map of PartitionInfo

CollectionInfo -> PartitionInfo -> ServerInfo

collectionName (string) -> collectionId (u32)


### schema management

collection schema information  (only the indexed fields) is recorded in the master and pushed into every partition.  

### scheduling polices

try to place partitions of different collections onto different pservers for performance isolation. 


## PartitionServer 

### interface

offload, load

### data structures

u64 (collectionId ## partitionId) --> Partition

the core index engine - Simba, currently doc store in rocksdb + secondary/fulltext index in tantivy

### Simba - the core indexing library

Simba has several kinds of indexes: 

1) primary key index, i.e. the document store, pk -> doc

2) secondary index

3) full-text index

4) vector, etc. 


* two options: 

a) synchronous secondary/full-text indexing

<pk,iid>, <iid,doc>, <term, array of iid> in rocksdb and sk encoded as ordered


b) async secondary/full-text indexing: covered by fulltext library like tantivy. 

Right now we just implement option b for simplicity

* latch manager

each partition has its latch managmer, in memory, and on the leader only


### write IO path

say insert/update/upsert/delete/cwrite a document X, 

1, LatchManager.latch(x.pk)

2, read x from rocksdb if it is needed

3, return error if not meet conditions

3, submit to raft replication

4, apply to rocksdb, and to tantivy

5, LatchManager.release(x.pk)

6, return ok


write performance optimization: raft log on tmpfs


## execution flow of search

local merging (intra-ps) & global merging (inter-ps)



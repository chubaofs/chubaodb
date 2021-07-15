use crate::util::raft::RaftStorage;
use std::sync::Arc;
use crate::util::entity::Collection;
use crate::util::error::ASResult;
use crate::meta::entity_key;
use crate::util::coding;

pub struct MetaStorage {
    store: Arc<RaftStorage>,
}

impl MetaStorage {
    pub fn new(rs: Arc<RaftStorage>) -> MetaStore {
        MetaStorage { store: rs }
    }

    pub fn get_collection(&self, collection_name: &str) -> ASResult<Collection> {
        let value = self
            .store
            .get(entity_key::collection_name(collection_name).as_str())?;

        self.get_collection_by_id(coding::slice_u32(&value[..]))
    }

    pub fn get_collection_by_id(&self, collection_id: u32) -> ASResult<Collection> {
        self.meta_service
            .get(entity_key::collection(collection_id).as_str())
    }

    pub fn list_collections(&self) -> ASResult<Vec<Collection>> {
        self.meta_service
            .list(entity_key::collection_prefix().as_str())
    }

    pub fn update_server(&self, mut server: PServer) -> ASResult<PServer> {
        server.modify_time = current_millis();
        self.meta_service.put(&server)?;
        return Ok(server);
    }

    pub fn list_servers(&self) -> ASResult<Vec<PServer>> {
        self.meta_service
            .list(entity_key::pserver_prefix().as_str())
    }

    pub fn get_server(&self, server_addr: &str) -> ASResult<PServer> {
        self.meta_service
            .get(entity_key::pserver(server_addr).as_str())
    }
}
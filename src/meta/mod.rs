pub mod graphql;
pub mod server;
pub mod storage;
pub mod service;


pub mod entity_key {
    const PREFIX_PSERVER: &str = "/META/SERVER";
    const PREFIX_COLLECTION: &str = "/META/COLLECTION";
    const PREFIX_PARTITION: &str = "/META/PARTITION";
    const PREFIX_PSERVER_ID: &str = "/META/SERVER_ID";

    pub const SEQ_COLLECTION: &str = "/META/SEQUENCE/COLLECTION";
    pub const SEQ_PARTITION: &str = "/META/SEQUENCE/PARTITION";
    pub const SEQ_PSERVER: &str = "/META/SEQUENCE/PSERVER";

    pub fn pserver(addr: &str) -> String {
        format!("{}/{}", PREFIX_PSERVER, addr)
    }

    pub fn pserver_prefix() -> String {
        format!("{}/", PREFIX_PSERVER)
    }

    pub fn pserver_id(server_id: u32) -> String {
        format!("{}/{}", PREFIX_PSERVER_ID, server_id)
    }

    pub fn collection(id: u32) -> String {
        format!("{}/{}", PREFIX_COLLECTION, id)
    }

    pub fn collection_prefix() -> String {
        format!("{}/", PREFIX_COLLECTION)
    }

    pub fn partiition(collection_id: u32, partiition_id: u32) -> String {
        format!("{}/{}/{}", PREFIX_PARTITION, collection_id, partiition_id)
    }

    pub fn partition_prefix(collection_id: u32) -> String {
        format!("{}/{}/", PREFIX_PARTITION, collection_id)
    }

    /// META_MAPPING_COLLECTION_{collection_name}
    pub fn collection_name(collection_name: &str) -> String {
        format!("META/MAPPING/COLLECTION/{}", collection_name)
    }

}
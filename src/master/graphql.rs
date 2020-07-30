use crate::master::service::MasterService;
use crate::util::{config, entity::*};
use async_graphql::*;
use log::{error, info, warn};
use serde_json::json;
use std::sync::Arc;

pub type JsonValue = Json<serde_json::Value>;
pub type MasterSchema = Schema<Query, Mutation, EmptySubscription>;

#[InputObject]
pub struct Fields {
    pub int: Option<Vec<IntField>>,
    pub float: Option<Vec<FloatField>>,
    pub string: Option<Vec<StringField>>,
    pub text: Option<Vec<TextField>>,
    pub vector: Option<Vec<VectorField>>,
    pub date: Option<Vec<DateField>>,
}

impl Fields {
    fn to_collect(self) -> Vec<Field> {
        let mut field_vec = vec![];
        if let Some(arr) = self.int {
            field_vec.extend(arr.into_iter().map(|v| Field::int(v)));
        }
        if let Some(arr) = self.float {
            field_vec.extend(arr.into_iter().map(|v| Field::float(v)));
        }
        if let Some(arr) = self.string {
            field_vec.extend(arr.into_iter().map(|v| Field::string(v)));
        }
        if let Some(arr) = self.text {
            field_vec.extend(arr.into_iter().map(|v| Field::text(v)));
        }
        if let Some(arr) = self.vector {
            field_vec.extend(arr.into_iter().map(|v| Field::vector(v)));
        }
        if let Some(arr) = self.date {
            field_vec.extend(arr.into_iter().map(|v| Field::date(v)));
        }
        field_vec
    }
}

pub struct Mutation;

#[Object]
impl Mutation {
    async fn pserver_register(&self, ctx: &Context<'_>, json: JsonValue) -> FieldResult<JsonValue> {
        info!("recive pserver_register begin");

        let ps: PServer = serde_json::from_value(json.0)
            .map_err(|e| FieldError(format!("unmarshal pserver has err:[{}]", e), None))?;

        let (addr, zone) = (ps.addr.clone(), ps.zone.clone());

        info!("prepare to heartbeat with address {}, zone {}", addr, zone);

        let service = ctx.data_unchecked::<Arc<MasterService>>();
        let mut ps = match service.register(ps) {
            Ok(s) => s,
            Err(e) => {
                error!(
                    "get server failed, zone:{}, server_addr:{}, err:{}",
                    addr,
                    zone,
                    e.to_string()
                );
                return Err(FieldError(
                    format!(
                        "pserver register failed, zone: {}, addr: {}, err: {}",
                        addr, zone, e
                    ),
                    None,
                ));
            }
        };

        let mut active_ids = Vec::new();

        for wp in ps.write_partitions {
            match service.get_partition(wp.collection_id, wp.id) {
                Ok(dbc) => {
                    if dbc.leader == ps.addr && dbc.load_term() <= wp.load_term() {
                        active_ids.push(dbc);
                    } else {
                        warn!(
                            "partition not load because not expected:{:?} found:{:?}",
                            dbc, wp
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "pserver for collection:{} partition:{} get has err:{:?}",
                        wp.collection_id, wp.id, e
                    );
                }
            }
        }

        ps.write_partitions = active_ids;

        Ok(Json(serde_json::to_value(ps)?))
    }

    async fn collection_create(
        &self,
        ctx: &Context<'_>,
        name: String,
        partition_num: i32,
        partition_replica_num: i32,
        fields: Option<Fields>,
    ) -> FieldResult<JsonValue> {
        let mut fs = vec![];
        if fields.is_some() {
            fs = fields.unwrap().to_collect();
        }

        let info = Collection {
            id: 0,
            name: name,
            fields: fs,
            partition_num: partition_num as u32,
            partition_replica_num: partition_replica_num as u32,
            status: CollectionStatus::UNKNOW,
            ..Default::default()
        };

        let v = serde_json::to_string(&info)?;
        info!("create collection:[{}]", v);

        let name = info.name.clone();
        info!("prepare to create collection with name {}", name);
        match ctx
            .data_unchecked::<Arc<MasterService>>()
            .create_collection(info)
            .await
        {
            Ok(s) => return Ok(Json(serde_json::to_value(s)?)),
            Err(e) => {
                error!(
                    "create collection failed, collection_name: {}, err: {}",
                    name, e
                );
                return Err(FieldError(
                    format!(
                        "create collection failed, collection_name: {}, err: {}",
                        name, e
                    ),
                    None,
                ));
            }
        }
    }

    async fn collection_hibernate(
        &self,
        ctx: &Context<'_>,
        name: String,
    ) -> FieldResult<JsonValue> {
        info!("prepare to delete collection name {}", name);

        match ctx
            .data_unchecked::<Arc<MasterService>>()
            .hibernate_collection(&name)
            .await
        {
            Ok(s) => Ok(Json(json!({
                "success":true,
                "collection":s
            }))),
            Err(e) => {
                error!(
                    "delete collection failed, collection_name {}, err: {}",
                    name,
                    e.to_string()
                );
                Err(FieldError(
                    format!(
                        "delete collection failed, collection_name: {}, err: {}",
                        name, e
                    ),
                    None,
                ))
            }
        }
    }

    async fn collection_delete(&self, ctx: &Context<'_>, name: String) -> FieldResult<JsonValue> {
        info!("prepare to delete collection name {}", name);

        match ctx
            .data_unchecked::<Arc<MasterService>>()
            .del_collection(&name)
            .await
        {
            Ok(s) => Ok(Json(json!({
                "success":true,
                "collection":s
            }))),
            Err(e) => {
                error!(
                    "delete collection failed, collection_name {}, err: {}",
                    name,
                    e.to_string()
                );
                Err(FieldError(
                    format!(
                        "delete collection failed, collection_name: {}, err: {}",
                        name, e
                    ),
                    None,
                ))
            }
        }
    }

    async fn pserver_update(&self, ctx: &Context<'_>, json: JsonValue) -> FieldResult<JsonValue> {
        let info: PServer = serde_json::from_value(json.0)?;
        info!(
            "prepare to update pserver with address {}, zone {}",
            info.addr, info.zone
        );
        match ctx
            .data_unchecked::<Arc<MasterService>>()
            .update_server(info)
        {
            Ok(s) => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::to_value(s.clone()).unwrap())
                        .unwrap()
                );
                return Ok(Json(serde_json::to_value(s)?));
            }
            Err(e) => {
                error!("update server failed, err: {}", e.to_string());
                return Err(FieldError(e.to_string(), None));
            }
        }
    }

    async fn partition_update(&self, ctx: &Context<'_>, json: JsonValue) -> FieldResult<JsonValue> {
        let info: Partition = serde_json::from_value(json.0)
            .map_err(|e| FieldError(format!("unmarshal partition has err:[{}]", e), None))?;

        info!(
            "prepare to update collection {} partition {}  to {}",
            info.collection_id, info.id, info.leader
        );
        match ctx
            .data_unchecked::<Arc<MasterService>>()
            .update_partition(info)
            .await
        {
            Ok(s) => Ok(Json(serde_json::to_value(s)?)),
            Err(e) => {
                let message = format!("update partition failed, err:{}", e.to_string());
                error!("{}", message);
                Err(FieldError(message, None))
            }
        }
    }
}

pub struct Query;

#[Object]
impl Query {
    async fn collection_list(&self, ctx: &Context<'_>) -> FieldResult<JsonValue> {
        return Ok(Json(serde_json::to_value(
            ctx.data_unchecked::<Arc<MasterService>>()
                .list_collections()?,
        )?));
    }

    async fn collection_get(
        &self,
        ctx: &Context<'_>,
        id: Option<i32>,
        name: Option<String>,
    ) -> FieldResult<JsonValue> {
        if let Some(collection_id) = id {
            info!("prepare to get collection by name {}", collection_id);
            match ctx
                .data_unchecked::<Arc<MasterService>>()
                .get_collection_by_id(collection_id as u32)
            {
                Ok(s) => return Ok(Json(serde_json::to_value(s)?)),
                Err(e) => {
                    error!(
                        "get collection failed, collection_id: {}, err: {}",
                        collection_id,
                        e.to_string()
                    );
                    return Err(FieldError(e.to_string(), None));
                }
            }
        };

        if let Some(collection_name) = name {
            info!("prepare to get collection by name {}", collection_name);
            match ctx
                .data_unchecked::<Arc<MasterService>>()
                .get_collection(&collection_name)
            {
                Ok(s) => return Ok(Json(serde_json::to_value(s)?)),
                Err(e) => {
                    error!(
                        "get collection failed collection_name: {}, err: {}",
                        collection_name,
                        e.to_string()
                    );
                    return Err(FieldError(e.to_string(), None));
                }
            }
        };

        return Err(FieldError(
            String::from("There must be one id or name"),
            None,
        ));
    }

    async fn version(&self, _: &Context<'_>) -> JsonValue {
        Json(serde_json::json!({
            "chubaodb":"master runing",
            "version":config::VERSION,
            "git_version": config::GIT_VERSION,
        }))
    }

    async fn pserver_list(&self, ctx: &Context<'_>) -> FieldResult<JsonValue> {
        Ok(Json(serde_json::to_value(
            ctx.data_unchecked::<Arc<MasterService>>()
                .list_servers()
                .unwrap(),
        )?))
    }

    async fn pserver_get(&self, ctx: &Context<'_>, id: u32) -> FieldResult<JsonValue> {
        info!("prepare to get pservers addr by server id");
        let service = ctx.data_unchecked::<Arc<MasterService>>();

        let addr = match service.get_server_addr(id) {
            Ok(s) => s,
            Err(e) => {
                let info = format!("get server by id has err, err: {}", e.to_string());
                error!("{}", &info);
                return Err(FieldError(info, None));
            }
        };

        match service.get_server(addr.as_str()) {
            Ok(s) => return Ok(Json(serde_json::to_value(s)?)),
            Err(e) => {
                error!("get server failed, id:{}, err:{}", id, e.to_string());
                Err(FieldError(e.to_string(), None))
            }
        }
    }

    //partition
    async fn partition_get(
        &self,
        ctx: &Context<'_>,
        collection_id: i32,
        partition_id: i32,
    ) -> FieldResult<JsonValue> {
        info!(
            "prepare to get partition by collection ID {}, partition ID {}",
            collection_id, partition_id
        );

        match ctx
            .data_unchecked::<Arc<MasterService>>()
            .get_partition(collection_id as u32, partition_id as u32)
        {
            Ok(s) => return Ok(Json(serde_json::to_value(s)?)),
            Err(e) => {
                error!(
                    "get partition failed, collection_id:{}, partition_id:{}, err:{}",
                    collection_id,
                    partition_id,
                    e.to_string()
                );
                Err(FieldError(e.to_string(), None))
            }
        }
    }

    async fn partition_list(
        &self,
        ctx: &Context<'_>,
        collection_name: String,
    ) -> FieldResult<JsonValue> {
        info!(
            "prepare to list partitions with collection name {}",
            &collection_name
        );

        match ctx
            .data_unchecked::<Arc<MasterService>>()
            .list_partitions(&collection_name)
        {
            Ok(s) => return Ok(Json(serde_json::to_value(s)?)),
            Err(e) => {
                error!("list partition failed, err: {}", e.to_string());
                Err(FieldError(e.to_string(), None))
            }
        }
    }
}

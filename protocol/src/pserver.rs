#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct CountDocumentRequest {
    #[prost(uint64, repeated, tag = "1")]
    pub cpids: ::prost::alloc::vec::Vec<u64>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct CountDocumentResponse {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(uint64, tag = "3")]
    pub estimate_count: u64,
    #[prost(uint64, tag = "4")]
    pub db_count: u64,
    #[prost(uint64, tag = "5")]
    pub index_count: u64,
    #[prost(message, repeated, tag = "6")]
    pub vectors_count: ::prost::alloc::vec::Vec<VectorCount>,
    #[prost(string, tag = "7")]
    pub message: ::prost::alloc::string::String,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct VectorCount {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub count: u64,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct Order {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub order: ::prost::alloc::string::String,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct AggCount {
    #[prost(uint64, tag = "1")]
    pub count: u64,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct AggStats {
    #[prost(uint64, tag = "1")]
    pub count: u64,
    #[prost(string, tag = "2")]
    pub field: ::prost::alloc::string::String,
    #[prost(double, tag = "3")]
    pub max: f64,
    #[prost(double, tag = "4")]
    pub min: f64,
    #[prost(double, tag = "5")]
    pub sum: f64,
    #[prost(uint64, tag = "6")]
    pub missing: u64,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct AggHits {
    #[prost(uint64, tag = "1")]
    pub size: u64,
    #[prost(uint64, tag = "2")]
    pub count: u64,
    #[prost(message, repeated, tag = "3")]
    pub hits: ::prost::alloc::vec::Vec<Hit>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct AggValue {
    #[prost(oneof = "agg_value::AggValue", tags = "1, 2, 3")]
    pub agg_value: ::core::option::Option<agg_value::AggValue>,
}
/// Nested message and enum types in `AggValue`.
pub mod agg_value {
    #[derive(
        serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Oneof,
    )]
    pub enum AggValue {
        #[prost(message, tag = "1")]
        Count(super::AggCount),
        #[prost(message, tag = "2")]
        Stats(super::AggStats),
        #[prost(message, tag = "3")]
        Hits(super::AggHits),
    }
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct AggValues {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub values: ::prost::alloc::vec::Vec<AggValue>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct AggregationResponse {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(uint64, tag = "2")]
    pub total: u64,
    #[prost(uint32, tag = "3")]
    pub size: u32,
    #[prost(message, repeated, tag = "4")]
    pub result: ::prost::alloc::vec::Vec<AggValues>,
    #[prost(message, optional, tag = "5")]
    pub info: ::core::option::Option<SearchInfo>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct QueryRequest {
    #[prost(uint64, repeated, tag = "1")]
    pub cpids: ::prost::alloc::vec::Vec<u64>,
    #[prost(string, tag = "2")]
    pub query: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub def_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "4")]
    pub vector_query: ::core::option::Option<VectorQuery>,
    #[prost(uint32, tag = "5")]
    pub size: u32,
    #[prost(message, repeated, tag = "6")]
    pub sort: ::prost::alloc::vec::Vec<Order>,
    #[prost(string, tag = "7")]
    pub group: ::prost::alloc::string::String,
    #[prost(string, tag = "8")]
    pub fun: ::prost::alloc::string::String,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct VectorQuery {
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    #[prost(float, repeated, tag = "2")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct SearchDocumentResponse {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(uint64, tag = "2")]
    pub total: u64,
    #[prost(message, repeated, tag = "3")]
    pub hits: ::prost::alloc::vec::Vec<Hit>,
    #[prost(message, optional, tag = "4")]
    pub info: ::core::option::Option<SearchInfo>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct SearchInfo {
    #[prost(int32, tag = "1")]
    pub success: i32,
    #[prost(int32, tag = "2")]
    pub error: i32,
    #[prost(string, tag = "3")]
    pub message: ::prost::alloc::string::String,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct GetDocumentRequest {
    #[prost(uint32, tag = "1")]
    pub collection_id: u32,
    #[prost(uint32, tag = "2")]
    pub partition_id: u32,
    #[prost(string, tag = "3")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub sort_key: ::prost::alloc::string::String,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct DocumentResponse {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "3")]
    pub doc: ::prost::alloc::vec::Vec<u8>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct Hit {
    #[prost(string, tag = "1")]
    pub collection_name: ::prost::alloc::string::String,
    #[prost(float, tag = "2")]
    pub score: f32,
    #[prost(bytes = "vec", tag = "3")]
    pub doc: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    pub sort: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct Document {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub sort_key: ::prost::alloc::string::String,
    #[prost(int64, tag = "3")]
    pub version: i64,
    #[prost(uint32, tag = "4")]
    pub slot: u32,
    #[prost(uint32, tag = "5")]
    pub partition_id: u32,
    #[prost(bytes = "vec", tag = "6")]
    pub source: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "7")]
    pub vectors: ::prost::alloc::vec::Vec<Vector>,
    #[prost(bytes = "vec", repeated, tag = "8")]
    pub scalars: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct Vector {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(float, repeated, tag = "2")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct WriteDocumentRequest {
    #[prost(uint32, tag = "1")]
    pub collection_id: u32,
    #[prost(uint32, tag = "2")]
    pub partition_id: u32,
    #[prost(message, optional, tag = "3")]
    pub doc: ::core::option::Option<Document>,
    #[prost(enumeration = "WriteType", tag = "4")]
    pub write_type: i32,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct ReplicaInfo {
    #[prost(uint32, tag = "1")]
    pub node: u32,
    #[prost(uint32, tag = "2")]
    pub replica_type: u32,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct PartitionRequest {
    #[prost(uint32, tag = "1")]
    pub partition_id: u32,
    #[prost(uint32, tag = "2")]
    pub collection_id: u32,
    #[prost(bool, tag = "3")]
    pub readonly: bool,
    /// if term ==0 not check term .
    #[prost(uint64, tag = "4")]
    pub term: u64,
    #[prost(message, repeated, tag = "5")]
    pub replicas: ::prost::alloc::vec::Vec<ReplicaInfo>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct StatsResponse {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(uint32, tag = "2")]
    pub collection_id: u32,
    #[prost(uint32, tag = "3")]
    pub partition_id: u32,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct GeneralRequest {
    #[prost(uint32, tag = "2")]
    pub collection_id: u32,
    #[prost(uint32, tag = "1")]
    pub partition_id: u32,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct GeneralResponse {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct CommandRequest {
    #[prost(bytes = "vec", tag = "2")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct CommandResponse {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "3")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
#[derive(
    serde_derive::Serialize,
    serde_derive::Deserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    ::prost::Enumeration,
)]
#[repr(i32)]
pub enum WriteType {
    Unknow = 0,
    Put = 1,
    Create = 2,
    Update = 3,
    Upsert = 4,
    Delete = 5,
}
#[doc = r" Generated client implementations."]
pub mod rpc_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct RpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RpcClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> RpcClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = " document handler"]
        pub async fn write(
            &mut self,
            request: impl tonic::IntoRequest<super::WriteDocumentRequest>,
        ) -> Result<tonic::Response<super::GeneralResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pserver.Rpc/Write");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetDocumentRequest>,
        ) -> Result<tonic::Response<super::DocumentResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pserver.Rpc/Get");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn search(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryRequest>,
        ) -> Result<tonic::Response<super::SearchDocumentResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pserver.Rpc/Search");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn agg(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryRequest>,
        ) -> Result<tonic::Response<super::AggregationResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pserver.Rpc/Agg");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn count(
            &mut self,
            request: impl tonic::IntoRequest<super::CountDocumentRequest>,
        ) -> Result<tonic::Response<super::CountDocumentResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pserver.Rpc/Count");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " ps handler"]
        pub async fn status(
            &mut self,
            request: impl tonic::IntoRequest<super::GeneralRequest>,
        ) -> Result<tonic::Response<super::GeneralResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pserver.Rpc/Status");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn load_partition(
            &mut self,
            request: impl tonic::IntoRequest<super::PartitionRequest>,
        ) -> Result<tonic::Response<super::GeneralResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pserver.Rpc/LoadPartition");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn offload_partition(
            &mut self,
            request: impl tonic::IntoRequest<super::PartitionRequest>,
        ) -> Result<tonic::Response<super::GeneralResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/pserver.Rpc/OffloadPartition");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for RpcClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for RpcClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RpcClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod rpc_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with RpcServer."]
    #[async_trait]
    pub trait Rpc: Send + Sync + 'static {
        #[doc = " document handler"]
        async fn write(
            &self,
            request: tonic::Request<super::WriteDocumentRequest>,
        ) -> Result<tonic::Response<super::GeneralResponse>, tonic::Status>;
        async fn get(
            &self,
            request: tonic::Request<super::GetDocumentRequest>,
        ) -> Result<tonic::Response<super::DocumentResponse>, tonic::Status>;
        async fn search(
            &self,
            request: tonic::Request<super::QueryRequest>,
        ) -> Result<tonic::Response<super::SearchDocumentResponse>, tonic::Status>;
        async fn agg(
            &self,
            request: tonic::Request<super::QueryRequest>,
        ) -> Result<tonic::Response<super::AggregationResponse>, tonic::Status>;
        async fn count(
            &self,
            request: tonic::Request<super::CountDocumentRequest>,
        ) -> Result<tonic::Response<super::CountDocumentResponse>, tonic::Status>;
        #[doc = " ps handler"]
        async fn status(
            &self,
            request: tonic::Request<super::GeneralRequest>,
        ) -> Result<tonic::Response<super::GeneralResponse>, tonic::Status>;
        async fn load_partition(
            &self,
            request: tonic::Request<super::PartitionRequest>,
        ) -> Result<tonic::Response<super::GeneralResponse>, tonic::Status>;
        async fn offload_partition(
            &self,
            request: tonic::Request<super::PartitionRequest>,
        ) -> Result<tonic::Response<super::GeneralResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct RpcServer<T: Rpc> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Rpc> RpcServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for RpcServer<T>
    where
        T: Rpc,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/pserver.Rpc/Write" => {
                    #[allow(non_camel_case_types)]
                    struct WriteSvc<T: Rpc>(pub Arc<T>);
                    impl<T: Rpc> tonic::server::UnaryService<super::WriteDocumentRequest> for WriteSvc<T> {
                        type Response = super::GeneralResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::WriteDocumentRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).write(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = WriteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/pserver.Rpc/Get" => {
                    #[allow(non_camel_case_types)]
                    struct GetSvc<T: Rpc>(pub Arc<T>);
                    impl<T: Rpc> tonic::server::UnaryService<super::GetDocumentRequest> for GetSvc<T> {
                        type Response = super::DocumentResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetDocumentRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/pserver.Rpc/Search" => {
                    #[allow(non_camel_case_types)]
                    struct SearchSvc<T: Rpc>(pub Arc<T>);
                    impl<T: Rpc> tonic::server::UnaryService<super::QueryRequest> for SearchSvc<T> {
                        type Response = super::SearchDocumentResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::QueryRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).search(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = SearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/pserver.Rpc/Agg" => {
                    #[allow(non_camel_case_types)]
                    struct AggSvc<T: Rpc>(pub Arc<T>);
                    impl<T: Rpc> tonic::server::UnaryService<super::QueryRequest> for AggSvc<T> {
                        type Response = super::AggregationResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::QueryRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).agg(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AggSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/pserver.Rpc/Count" => {
                    #[allow(non_camel_case_types)]
                    struct CountSvc<T: Rpc>(pub Arc<T>);
                    impl<T: Rpc> tonic::server::UnaryService<super::CountDocumentRequest> for CountSvc<T> {
                        type Response = super::CountDocumentResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CountDocumentRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).count(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = CountSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/pserver.Rpc/Status" => {
                    #[allow(non_camel_case_types)]
                    struct StatusSvc<T: Rpc>(pub Arc<T>);
                    impl<T: Rpc> tonic::server::UnaryService<super::GeneralRequest> for StatusSvc<T> {
                        type Response = super::GeneralResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GeneralRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).status(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = StatusSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/pserver.Rpc/LoadPartition" => {
                    #[allow(non_camel_case_types)]
                    struct LoadPartitionSvc<T: Rpc>(pub Arc<T>);
                    impl<T: Rpc> tonic::server::UnaryService<super::PartitionRequest> for LoadPartitionSvc<T> {
                        type Response = super::GeneralResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PartitionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).load_partition(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = LoadPartitionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/pserver.Rpc/OffloadPartition" => {
                    #[allow(non_camel_case_types)]
                    struct OffloadPartitionSvc<T: Rpc>(pub Arc<T>);
                    impl<T: Rpc> tonic::server::UnaryService<super::PartitionRequest> for OffloadPartitionSvc<T> {
                        type Response = super::GeneralResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PartitionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).offload_partition(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = OffloadPartitionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Rpc> Clone for RpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Rpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Rpc> tonic::transport::NamedService for RpcServer<T> {
        const NAME: &'static str = "pserver.Rpc";
    }
}

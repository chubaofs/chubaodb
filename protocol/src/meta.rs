#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct GetMetricsRequest {}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct GetMetricsResponse {
    #[prost(message, optional, tag = "1")]
    pub metrics: ::core::option::Option<super::raft::RaftMetrics>,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct PServer {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, tag = "2")]
    pub rpc_addr: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub raft_addr: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub partitions: ::prost::alloc::vec::Vec<Partition>,
    #[prost(string, tag = "5")]
    pub zone: ::prost::alloc::string::String,
    #[prost(int64, tag = "6")]
    pub modify_time: i64,
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct Collection {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "collection::Type", tag = "3")]
    pub r#type: i32,
    #[prost(uint32, tag = "4")]
    pub partition_num: u32,
    #[prost(uint32, tag = "5")]
    pub partition_replica_num: u32,
    #[prost(uint32, repeated, tag = "6")]
    pub partition_ids: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, repeated, tag = "7")]
    pub fields: ::prost::alloc::vec::Vec<Field>,
    #[prost(int64, tag = "8")]
    pub create_time: i64,
    #[prost(int64, tag = "9")]
    pub modify_time: i64,
}
/// Nested message and enum types in `Collection`.
pub mod collection {
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
    pub enum Type {
        Unknow = 0,
        Creating = 1,
        Droped = 2,
        Working = 3,
    }
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct Field {
    #[prost(enumeration = "field::Type", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(bool, tag = "3")]
    pub array: bool,
    #[prost(bool, tag = "4")]
    pub none: bool,
    #[prost(bool, tag = "5")]
    pub sort: bool,
    #[prost(message, optional, tag = "6")]
    pub vector: ::core::option::Option<field::Vecotr>,
}
/// Nested message and enum types in `Field`.
pub mod field {
    #[derive(
        serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
    )]
    pub struct Vecotr {
        #[prost(int32, tag = "1")]
        pub train_size: i32,
        #[prost(int32, tag = "2")]
        pub dimension: i32,
        #[prost(enumeration = "vecotr::MetricType", tag = "3")]
        pub metric_type: i32,
    }
    /// Nested message and enum types in `Vecotr`.
    pub mod vecotr {
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
        pub enum MetricType {
            L2 = 0,
            InnerProduct = 1,
        }
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
    pub enum Type {
        Unknow = 0,
        Creating = 1,
        Droped = 2,
        Working = 3,
    }
}
#[derive(
    serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, ::prost::Message,
)]
pub struct Partition {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(uint32, tag = "2")]
    pub collection_id: u32,
    #[prost(uint32, tag = "3")]
    pub leader: u32,
    #[prost(uint32, repeated, tag = "4")]
    pub replicas: ::prost::alloc::vec::Vec<u32>,
    #[prost(int64, tag = "5")]
    pub modify_time: i64,
}
#[doc = r" Generated client implementations."]
pub mod meta_raft_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct MetaRaftClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MetaRaftClient<tonic::transport::Channel> {
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
    impl<T> MetaRaftClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + Sync + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> MetaRaftClient<InterceptedService<T, F>>
        where
            F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            MetaRaftClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn vote(
            &mut self,
            request: impl tonic::IntoRequest<super::super::raft::VoteRequest>,
        ) -> Result<tonic::Response<super::super::raft::VoteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta_raft.MetaRaft/Vote");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn append_entries(
            &mut self,
            request: impl tonic::IntoRequest<super::super::raft::AppendEntriesRequest>,
        ) -> Result<tonic::Response<super::super::raft::AppendEntriesResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta_raft.MetaRaft/AppendEntries");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn metrics(
            &mut self,
            request: impl tonic::IntoRequest<super::GetMetricsRequest>,
        ) -> Result<tonic::Response<super::GetMetricsResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta_raft.MetaRaft/Metrics");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod meta_raft_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with MetaRaftServer."]
    #[async_trait]
    pub trait MetaRaft: Send + Sync + 'static {
        async fn vote(
            &self,
            request: tonic::Request<super::super::raft::VoteRequest>,
        ) -> Result<tonic::Response<super::super::raft::VoteResponse>, tonic::Status>;
        async fn append_entries(
            &self,
            request: tonic::Request<super::super::raft::AppendEntriesRequest>,
        ) -> Result<tonic::Response<super::super::raft::AppendEntriesResponse>, tonic::Status>;
        async fn metrics(
            &self,
            request: tonic::Request<super::GetMetricsRequest>,
        ) -> Result<tonic::Response<super::GetMetricsResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MetaRaftServer<T: MetaRaft> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: MetaRaft> MetaRaftServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for MetaRaftServer<T>
    where
        T: MetaRaft,
        B: Body + Send + Sync + 'static,
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
                "/meta_raft.MetaRaft/Vote" => {
                    #[allow(non_camel_case_types)]
                    struct VoteSvc<T: MetaRaft>(pub Arc<T>);
                    impl<T: MetaRaft> tonic::server::UnaryService<super::super::raft::VoteRequest> for VoteSvc<T> {
                        type Response = super::super::raft::VoteResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::raft::VoteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).vote(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = VoteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta_raft.MetaRaft/AppendEntries" => {
                    #[allow(non_camel_case_types)]
                    struct AppendEntriesSvc<T: MetaRaft>(pub Arc<T>);
                    impl<T: MetaRaft>
                        tonic::server::UnaryService<super::super::raft::AppendEntriesRequest>
                        for AppendEntriesSvc<T>
                    {
                        type Response = super::super::raft::AppendEntriesResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::raft::AppendEntriesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).append_entries(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AppendEntriesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta_raft.MetaRaft/Metrics" => {
                    #[allow(non_camel_case_types)]
                    struct MetricsSvc<T: MetaRaft>(pub Arc<T>);
                    impl<T: MetaRaft> tonic::server::UnaryService<super::GetMetricsRequest> for MetricsSvc<T> {
                        type Response = super::GetMetricsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetMetricsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).metrics(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = MetricsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
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
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: MetaRaft> Clone for MetaRaftServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: MetaRaft> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MetaRaft> tonic::transport::NamedService for MetaRaftServer<T> {
        const NAME: &'static str = "meta_raft.MetaRaft";
    }
}

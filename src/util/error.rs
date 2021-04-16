use crate::pserverpb::*;
use tracing::log::error;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde_json::json;
use std::convert::TryFrom;
use std::error::Error;

pub type ASResult<T> = std::result::Result<T, ASError>;

#[macro_export]
macro_rules! err {
    ($code:expr , $arg:expr) => {{
        use std::convert::TryFrom;
        use tracing::log::log_enabled;
        use tracing::log::Level::Debug;
        let code = match Code::try_from($code) {
            Ok(c) => c,
            Err(_) => Code::InvalidErr,
        };

        if log_enabled!(Debug) {
            ASError::Error(code, format!("{} trace:{:?}", $arg, ASError::trace()))
        }else{
            ASError::Error(code, format!("{}", $arg))
        }

    }};
    ($code:expr , $($arg:tt)*) => {{
        use std::convert::TryFrom;
        use tracing::log::log_enabled;
        use tracing::log::Level::Debug;
        let code = match Code::try_from($code) {
            Ok(c) => c,
            Err(_) => Code::InvalidErr,
        };
        if log_enabled!(Debug) {
            let msg = format!($($arg)*) ;
            ASError::Error(code, format!("{} trace:{:?}", msg, ASError::trace()))
        }else{
            ASError::Error(code, format!($($arg)*))
        }
    }};
}

#[macro_export]
macro_rules! err_def {
    ($arg:expr) => {{
        use tracing::log::log_enabled;
        use tracing::log::Level::Debug;
        if log_enabled!(Debug) {
            ASError::Error(Code::InternalErr, format!("{} trace:{:?}", $arg, ASError::trace()))
        }else{
            ASError::Error(Code::InternalErr, format!("{}", $arg))
        }
    }};
    ($($arg:tt)*) => {{
        use tracing::log::log_enabled;
        use tracing::log::Level::Debug;
        if log_enabled!(Debug) {
            let msg = format!($($arg)*) ;
            ASError::Error(Code::InternalErr, format!("{} trace:{:?}", msg, ASError::trace()))
        }else{
            ASError::Error(Code::InternalErr, format!($($arg)*))
        }
    }};
}

#[macro_export]
macro_rules! result {
    ($code:expr , $arg:expr) => {{
        Err(err!($code, $arg))
    }};
    ($code:expr , $($arg:tt)*) => {{
        Err(err!($code, $($arg)*))
    }};
}

#[macro_export]
macro_rules! result_def {
    ($arg:expr) => {{
        Err(err_def!($arg))
    }};
    ($($arg:tt)*) => {{
        Err(err_def!($($arg)*))
    }};
}

#[macro_export]
macro_rules! result_obj_code {
    ($obj:expr) => {{
        use std::convert::TryFrom;
        let code = match Code::try_from($obj.code) {
            Ok(c) => c,
            Err(_) => Code::InvalidErr,
        };

        if code != Code::Success {
            return result!(code, $obj.message);
        } else {
            return Ok($obj);
        }
    }};
}

pub fn conver<T, E: std::fmt::Display>(result: Result<T, E>) -> ASResult<T> {
    match result {
        Ok(t) => Ok(t),
        Err(e) => Err(ASError::Error(Code::InternalErr, format!("{}", e))),
    }
}

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive, Clone, Copy)]
#[repr(i32)]
pub enum Code {
    Success = 200,
    InternalErr = 550,
    DocumentNotFound,
    AlreadyExists,
    InvalidErr,
    ParamError,
    EngineNotReady,
    EngineWillClose,
    RocksDBNotFound,
    VersionErr,
    PServerNotFound,
    CollectionNoIndex,
    CollectionNotFound,
    PartitionNotLeader,
    PartitionNotInit,
    PartitionLoadErr,
    PartitionNotFound,
    FieldTypeErr,
    FieldValueErr,
    LockedAlready,
    LockedLeaseExpried,
    HttpAPIRequestErr,
    EncodingErr,
    DencodingErr,
    Timeout,
    SerdeErr,
}

impl Code {
    pub fn http_code(self) -> http::status::StatusCode {
        let code: i32 = self.into();
        match http::status::StatusCode::from_u16(code as u16) {
            Ok(v) => v,
            Err(_) => {
                error!("the code:[{:?}] can not to http code", code);
                http::status::StatusCode::from_u16(551).unwrap()
            }
        }
    }

    pub fn from_i32(w: i32) -> Code {
        match Code::try_from(w) {
            Ok(c) => c,
            Err(_) => Code::InvalidErr,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ASError {
    Success,
    Error(Code, String),
}

impl ASError {
    pub fn code(&self) -> Code {
        match self {
            ASError::Success => Code::Success,
            ASError::Error(c, _) => *c,
        }
    }

    pub fn message(&self) -> String {
        match self {
            ASError::Success => String::from("success"),
            ASError::Error(_, s) => s.clone(),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            ASError::Success => json!({
                "code":200,
                "message":"success",
            }),
            ASError::Error(c, m) => json!({
                "code": *c as i32,
                "message": m
            }),
        }
    }

    pub fn trace() -> Vec<String> {
        let mut list = Vec::new();
        backtrace::trace(|frame| {
            // Resolve this instruction pointer to a symbol name
            backtrace::resolve_frame(frame, |symbol| {
                let line = format!(
                    "name:{:?} line:{:?}",
                    symbol.name(),
                    symbol.lineno().unwrap_or(0)
                );

                if line.contains("chubaodb") {
                    list.push(line);
                }
            });
            true // keep going to the next frame
        });
        list
    }
}

impl std::fmt::Display for ASError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ASError::Error(code, msg) => write!(f, "code:{:?} -> err:[{}]", code, msg),
            ASError::Success => write!(f, "code:200 -> success"),
        }
    }
}


impl From<serde_json::Error> for ASError {
    fn from(e: serde_json::Error) -> Self {
        err!(Code::SerdeErr, e.to_string())
    }
}

impl From<prost::DecodeError> for ASError {
    fn from(e: prost::DecodeError) -> Self {
        err!(Code::DencodingErr, e.to_string())
    }
}

impl From<prost::EncodeError> for ASError {
    fn from(e: prost::EncodeError) -> Self {
        err!(Code::EncodingErr, e.to_string())
    }
}

impl From<tantivy::directory::error::OpenDirectoryError> for ASError {
    fn from(e: tantivy::directory::error::OpenDirectoryError) -> Self {
        err!(Code::InternalErr, e.to_string())
    }
}

impl From<std::io::Error> for ASError {
    fn from(e: std::io::Error) -> Self {
        err!(Code::InternalErr, e.to_string())
    }
}

impl From<rocksdb::Error> for ASError {
    fn from(e: rocksdb::Error) -> Self {
        err!(Code::InternalErr, e.to_string())
    }
}

impl From<http::uri::InvalidUri> for ASError {
    fn from(e: http::uri::InvalidUri) -> Self {
        err!(Code::ParamError, e.to_string())
    }
}

impl From<tonic::transport::Error> for ASError {
    fn from(e: tonic::transport::Error) -> Self {
        err!(Code::InternalErr, e.to_string())
    }
}

impl From<reqwest::Error> for ASError {
    fn from(e: reqwest::Error) -> Self {
        err!(Code::InternalErr, e.to_string())
    }
}

impl From<tonic::Status> for ASError {
    fn from(e: tonic::Status) -> Self {
        err!(Code::InternalErr, e.to_string())
    }
}


pub fn cast<E: std::fmt::Display>(e: E) -> ASError {
    ASError::Error(Code::InternalErr, e.to_string())
}

impl Into<SearchDocumentResponse> for ASError {
    fn into(self) -> SearchDocumentResponse {
        SearchDocumentResponse {
            code: self.code().into(),
            total: 0,
            hits: vec![],
            info: Some(SearchInfo {
                error: 1,
                success: 0,
                message: self.to_string(),
            }),
        }
    }
}


impl Into<AggregationResponse> for ASError {
    fn into(self) -> AggregationResponse {
        AggregationResponse {
            code: self.code().into(),
            total: 0,
            size: 0,
            result: vec![],
            info: Some(SearchInfo {
                error: 1,
                success: 0,
                message: self.to_string(),
            }),
        }
    }
}

impl Into<GeneralResponse> for ASError {
    fn into(self) -> GeneralResponse {
        GeneralResponse {
            code: self.code().into(),
            message: self.to_string(),
        }
    }
}

impl Into<DocumentResponse> for ASError {
    fn into(self) -> DocumentResponse {
        DocumentResponse {
            code: self.code().into(),
            message: self.to_string(),
            doc: Vec::default(),
        }
    }
}

impl Into<CountDocumentResponse> for ASError {
    fn into(self) -> CountDocumentResponse {
        CountDocumentResponse {
            code: self.code().into(),
            message: self.to_string(),
            ..Default::default()
        }
    }
}

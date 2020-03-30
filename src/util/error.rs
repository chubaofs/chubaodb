// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.

use crate::util;
use http::status::StatusCode;
use serde_derive::{Deserialize, Serialize};
use serde_json::json;

// the code need  >= 100 &&  >= 600 {
pub const SUCCESS: u16 = 200;
pub const INTERNAL_ERR: u16 = 500;
pub const ENGINE_NOT_READY: u16 = 501;
pub const CONN_ERR: u16 = 502;
pub const NOT_FOUND: u16 = 503;
pub const ALREADY_EXISTS: u16 = 504;
pub const ENGINE_WILL_CLOSE: u16 = 505;
pub const VERSION_ERR: u16 = 506;
pub const PARTITION_NO_INDEX: u16 = 508;
pub const ENGINE_NOT_WRITABLE: u16 = 509;
//cluster_lock
pub const LOCKED_ALREADY: u16 = 514;
pub const LOCKED_LEASE_EXPRIED: u16 = 515;
pub const PARTITION_CAN_NOT_LOAD: u16 = 516;
pub const FIELD_TYPE_ERR: u16 = 517;
pub const INVALID: u16 = 599;
pub fn http_code(code: u16) -> StatusCode {
    match StatusCode::from_u16(code) {
        Ok(sc) => sc,
        Err(_) => StatusCode::from_u16(INVALID).ok().unwrap(),
    }
}

//this value for write return message
pub const ALL_WRITE_ERR_KEY: usize = 99999999;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GenericError(pub u16, pub String);

impl std::fmt::Display for GenericError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "code:{} -> err:[{}]", self.0, self.1)
    }
}

impl std::error::Error for GenericError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl GenericError {
    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "code": self.0,
            "message": self.1
        })
    }
}

pub type GResult<T> = std::result::Result<T, GenericError>;
pub type ASResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn err(info: String) -> GenericError {
    util::stack_trace();
    GenericError(INTERNAL_ERR, info)
}
pub fn err_code(code: u16, info: String) -> GenericError {
    util::stack_trace();
    GenericError(code, info.to_string())
}
pub fn err_code_str(code: u16, info: &str) -> GenericError {
    util::stack_trace();
    GenericError(code, info.to_string())
}
pub fn err_str(info: &str) -> GenericError {
    util::stack_trace();
    GenericError(INTERNAL_ERR, info.to_string())
}
pub fn err_generic() -> GenericError {
    util::stack_trace();
    GenericError(INTERNAL_ERR, String::from("internal err"))
}

pub fn err_box(info: String) -> Box<dyn std::error::Error> {
    Box::from(err(info))
}

pub fn err_code_box(code: u16, info: String) -> Box<dyn std::error::Error> {
    Box::from(err_code(code, info))
}

pub fn err_str_box(info: &str) -> Box<dyn std::error::Error> {
    Box::from(err_str(info))
}

pub fn err_code_str_box(code: u16, info: &str) -> Box<dyn std::error::Error> {
    Box::from(err_code_str(code, info))
}

pub fn err_generic_box() -> Box<dyn std::error::Error> {
    Box::from(err_generic())
}

pub fn convert<T, E: ToString>(result: Result<T, E>) -> ASResult<T> {
    match result {
        Ok(t) => Ok(t),
        Err(e) => Err(Box::from(err(e.to_string()))),
    }
}

pub fn cast_to_err(e: Box<dyn std::error::Error>) -> Box<GenericError> {
    match e.downcast::<GenericError>() {
        Ok(ge) => ge,
        Err(e) => Box::from(err_code(INTERNAL_ERR, e.to_string())),
    }
}

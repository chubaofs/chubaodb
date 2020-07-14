use crate::util::{
    coding::*,
    error::{ASError, ASResult, Code},
    time,
};
use crate::*;
use chrono::prelude::*;
use serde_json::Value;
use std::convert::TryInto;

pub enum Convert<'a> {
    String(String),
    Str(&'a str),
    Bytes(Vec<u8>),
    BytesRef(&'a [u8]),
    Bool(bool),
    U8(u8),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    Usize(usize),
    DateTime(NaiveDateTime),
}

impl From<String> for Convert<'_> {
    fn from(v: String) -> Self {
        Convert::String(v)
    }
}

impl<'a> From<&'a str> for Convert<'a> {
    fn from(v: &'a str) -> Self {
        Convert::Str(v)
    }
}

impl From<Vec<u8>> for Convert<'_> {
    fn from(v: Vec<u8>) -> Self {
        Convert::Bytes(v)
    }
}

impl<'a> From<&'a [u8]> for Convert<'a> {
    fn from(v: &'a [u8]) -> Self {
        Convert::BytesRef(v)
    }
}

impl From<bool> for Convert<'_> {
    fn from(v: bool) -> Self {
        Convert::Bool(v)
    }
}

impl From<u8> for Convert<'_> {
    fn from(v: u8) -> Self {
        Convert::U8(v)
    }
}

impl From<i32> for Convert<'_> {
    fn from(v: i32) -> Self {
        Convert::I32(v)
    }
}

impl From<u32> for Convert<'_> {
    fn from(v: u32) -> Self {
        Convert::U32(v)
    }
}

impl From<i64> for Convert<'_> {
    fn from(v: i64) -> Self {
        Convert::I64(v)
    }
}

impl From<u64> for Convert<'_> {
    fn from(v: u64) -> Self {
        Convert::U64(v)
    }
}

impl From<f64> for Convert<'_> {
    fn from(v: f64) -> Self {
        Convert::F64(v)
    }
}

impl From<f32> for Convert<'_> {
    fn from(v: f32) -> Self {
        Convert::F32(v)
    }
}

impl From<usize> for Convert<'_> {
    fn from(v: usize) -> Self {
        Convert::Usize(v)
    }
}

impl From<NaiveDateTime> for Convert<'_> {
    fn from(v: NaiveDateTime) -> Self {
        Convert::DateTime(v)
    }
}

impl TryInto<String> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            Convert::String(v) => Ok(v),
            Convert::Str(v) => Ok(v.to_string()),
            Convert::Bytes(v) => {
                String::from_utf8(v).map_err(|e| ASError::Error(Code::InternalErr, e.to_string()))
            }
            Convert::BytesRef(v) => String::from_utf8(v.to_vec())
                .map_err(|e| ASError::Error(Code::InternalErr, e.to_string())),
            Convert::Bool(v) => {
                if v {
                    Ok(String::from("true"))
                } else {
                    Ok(String::from("false"))
                }
            }
            Convert::U8(v) => Ok(format!("{}", v)),
            Convert::I32(v) => Ok(format!("{}", v)),
            Convert::U32(v) => Ok(format!("{}", v)),
            Convert::I64(v) => Ok(format!("{}", v)),
            Convert::U64(v) => Ok(format!("{}", v)),
            Convert::F32(v) => Ok(format!("{}", v)),
            Convert::F64(v) => Ok(format!("{}", v)),
            Convert::Usize(v) => Ok(format!("{}", v)),
            Convert::DateTime(v) => Ok(v.format("%Y-%m-%d %H:%M:%S").to_string()),
        }
    }
}

impl TryInto<Vec<u8>> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        match self {
            Convert::String(v) => Ok(v.into_bytes()),
            Convert::Str(v) => Ok(v.as_bytes().to_vec()),
            Convert::Bytes(v) => Ok(v),
            Convert::BytesRef(v) => Ok(v.to_vec()),
            Convert::Bool(v) => {
                if v {
                    Ok(vec![1])
                } else {
                    Ok(vec![0])
                }
            }
            Convert::U8(v) => Ok(vec![v]),
            Convert::I32(v) => Ok(v.to_be_bytes().to_vec()),
            Convert::U32(v) => Ok(v.to_be_bytes().to_vec()),
            Convert::I64(v) => Ok(v.to_be_bytes().to_vec()),
            Convert::U64(v) => Ok(v.to_be_bytes().to_vec()),
            Convert::F32(v) => Ok(v.to_be_bytes().to_vec()),
            Convert::F64(v) => Ok(v.to_be_bytes().to_vec()),
            Convert::Usize(v) => Ok(v.to_be_bytes().to_vec()),
            Convert::DateTime(v) => Ok(v.timestamp_millis().to_be_bytes().to_vec()),
        }
    }
}

impl TryInto<bool> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> ASResult<bool> {
        match self {
            Convert::String(v) => {
                if v.eq_ignore_ascii_case("true") {
                    Ok(true)
                } else if v.eq_ignore_ascii_case("false") {
                    Ok(false)
                } else {
                    Err(err!(Code::InternalErr, "{} can into bool", v))
                }
            }
            Convert::Str(v) => {
                if v.eq_ignore_ascii_case("true") {
                    Ok(true)
                } else if v.eq_ignore_ascii_case("false") {
                    Ok(false)
                } else {
                    Err(err!(Code::InternalErr, "{} can into bool", v))
                }
            }
            Convert::Bytes(v) => {
                if v.len() == 1 {
                    if v[0] == 0 {
                        return Ok(false);
                    } else if v[0] == 1 {
                        return Ok(true);
                    }
                }
                return Err(err!(Code::InternalErr, "{:?} can into bool", v));
            }
            Convert::BytesRef(v) => {
                if v.len() == 1 {
                    if v[0] == 0 {
                        return Ok(false);
                    } else if v[0] == 1 {
                        return Ok(true);
                    }
                }
                return Err(err!(Code::InternalErr, "{:?} can into bool", v));
            }
            Convert::Bool(v) => Ok(v),
            Convert::U8(v) => Ok(v == 1),
            Convert::I32(v) => Ok(v == 1),
            Convert::U32(v) => Ok(v == 1),
            Convert::I64(v) => Ok(v == 1),
            Convert::U64(v) => Ok(v == 1),
            Convert::F32(v) => Ok(v == 1.0),
            Convert::F64(v) => Ok(v == 1.0),
            Convert::Usize(v) => Ok(v == 1),
            Convert::DateTime(_) => Err(err!(Code::InternalErr, "date can into bool")),
        }
    }
}

impl TryInto<u8> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<u8, Self::Error> {
        match self {
            Convert::String(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to u8", v)),
            Convert::Str(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to u8", v)),
            Convert::Bytes(v) => {
                if v.len() == 1 {
                    return Ok(v[0]);
                }
                return Err(err!(Code::InternalErr, "{:?} can into u8", v));
            }
            Convert::BytesRef(v) => {
                if v.len() == 1 {
                    return Ok(v[0]);
                }
                return Err(err!(Code::InternalErr, "{:?} can into u8", v));
            }
            Convert::Bool(v) => {
                if v {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            Convert::U8(v) => Ok(v),
            Convert::I32(v) => {
                if v < 0 || v > 255 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u8`",
                        v
                    ))
                } else {
                    Ok(v as u8)
                }
            }
            Convert::U32(v) => {
                if v > 255 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u8`",
                        v
                    ))
                } else {
                    Ok(v as u8)
                }
            }
            Convert::I64(v) => {
                if v < 0 || v > 255 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u8`",
                        v
                    ))
                } else {
                    Ok(v as u8)
                }
            }
            Convert::U64(v) => {
                if v > 255 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u8`",
                        v
                    ))
                } else {
                    Ok(v as u8)
                }
            }
            Convert::F32(v) => {
                if v < 0.0 || v > 255.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u8`",
                        v
                    ))
                } else {
                    Ok(v as u8)
                }
            }
            Convert::F64(v) => {
                if v < 0.0 || v > 255.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u8`",
                        v
                    ))
                } else {
                    Ok(v as u8)
                }
            }
            Convert::Usize(v) => {
                if v > 255 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u8`",
                        v
                    ))
                } else {
                    Ok(v as u8)
                }
            }
            Convert::DateTime(v) => {
                let v = v.timestamp_millis();
                if v < 0 || v > 255 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u8`",
                        v
                    ))
                } else {
                    Ok(v as u8)
                }
            }
        }
    }
}

impl TryInto<i32> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<i32, Self::Error> {
        match self {
            Convert::String(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to i32", v)),
            Convert::Str(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to i32", v)),
            Convert::Bytes(v) => match v.len() {
                1 => Ok(v[0] as i32),
                4 => Ok(slice_i32(&v)),
                8 => {
                    let v = slice_i64(&v);
                    if v < -2147483648 || v > 2147483647 {
                        Err(err!(
                            Code::InternalErr,
                            "{:?} literal out of range for `i32`",
                            v
                        ))
                    } else {
                        Ok(v as i32)
                    }
                }
                _ => Err(err!(Code::InternalErr, "{:?} can into i32", v)),
            },
            Convert::BytesRef(v) => match v.len() {
                1 => Ok(v[0] as i32),
                4 => Ok(slice_i32(v)),
                8 => {
                    let v = slice_i64(v);
                    if v < -2147483648 || v > 2147483647 {
                        Err(err!(
                            Code::InternalErr,
                            "{:?} literal out of range for `i32`",
                            v
                        ))
                    } else {
                        Ok(v as i32)
                    }
                }
                _ => Err(err!(Code::InternalErr, "{:?} can into i32", v)),
            },
            Convert::Bool(v) => {
                if v {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            Convert::U8(v) => Ok(v as i32),
            Convert::I32(v) => Ok(v),
            Convert::U32(v) => {
                if v > 2147483647 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i32`",
                        v
                    ))
                } else {
                    Ok(v as i32)
                }
            }
            Convert::I64(v) => {
                if v < -2147483648 || v > 2147483647 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i32`",
                        v
                    ))
                } else {
                    Ok(v as i32)
                }
            }
            Convert::U64(v) => {
                if v > 2147483647 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i32`",
                        v
                    ))
                } else {
                    Ok(v as i32)
                }
            }
            Convert::F32(v) => {
                if v < -2147483648.0 || v > 2147483647.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i32`",
                        v
                    ))
                } else {
                    Ok(v as i32)
                }
            }
            Convert::F64(v) => {
                if v < -2147483648.0 || v > 2147483647.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i32`",
                        v
                    ))
                } else {
                    Ok(v as i32)
                }
            }
            Convert::Usize(v) => {
                if v > 2147483647 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i32`",
                        v
                    ))
                } else {
                    Ok(v as i32)
                }
            }
            Convert::DateTime(v) => {
                let v = v.timestamp_millis();
                if v < -2147483648 || v > 2147483647 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i32`",
                        v
                    ))
                } else {
                    Ok(v as i32)
                }
            }
        }
    }
}

impl TryInto<u32> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<u32, Self::Error> {
        match self {
            Convert::String(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to u32", v)),
            Convert::Str(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to u32", v)),
            Convert::Bytes(v) => match v.len() {
                1 => Ok(v[0] as u32),
                4 => Ok(slice_u32(&v)),
                8 => {
                    let v = slice_u64(&v);
                    if v > 4294967295 {
                        Err(err!(
                            Code::InternalErr,
                            "{:?} literal out of range for `u32`",
                            v
                        ))
                    } else {
                        Ok(v as u32)
                    }
                }
                _ => Err(err!(Code::InternalErr, "{:?} can into u32", v)),
            },
            Convert::BytesRef(v) => match v.len() {
                1 => Ok(v[0] as u32),
                4 => Ok(slice_u32(v)),
                8 => {
                    let v = slice_u64(v);
                    if v > 4294967295 {
                        Err(err!(
                            Code::InternalErr,
                            "{:?} literal out of range for `u32`",
                            v
                        ))
                    } else {
                        Ok(v as u32)
                    }
                }
                _ => Err(err!(Code::InternalErr, "{:?} can into u32", v)),
            },
            Convert::Bool(v) => {
                if v {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            Convert::U8(v) => Ok(v as u32),
            Convert::I32(v) => {
                if v < 0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u32`",
                        v
                    ))
                } else {
                    Ok(v as u32)
                }
            }
            Convert::U32(v) => Ok(v),
            Convert::I64(v) => {
                if v < 0 || v > 4294967295 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u32`",
                        v
                    ))
                } else {
                    Ok(v as u32)
                }
            }
            Convert::U64(v) => {
                if v > 4294967295 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u32`",
                        v
                    ))
                } else {
                    Ok(v as u32)
                }
            }
            Convert::F32(v) => {
                if v < 0.0 || v > 4294967295.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u32`",
                        v
                    ))
                } else {
                    Ok(v as u32)
                }
            }
            Convert::F64(v) => {
                if v < 0.0 || v > 4294967295.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u32`",
                        v
                    ))
                } else {
                    Ok(v as u32)
                }
            }
            Convert::Usize(v) => {
                if v > 4294967295 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u32`",
                        v
                    ))
                } else {
                    Ok(v as u32)
                }
            }
            Convert::DateTime(v) => {
                let v = v.timestamp_millis();
                if v < 0 || v > 4294967295 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u32`",
                        v
                    ))
                } else {
                    Ok(v as u32)
                }
            }
        }
    }
}

impl TryInto<i64> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<i64, Self::Error> {
        match self {
            Convert::String(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to i64", v)),
            Convert::Str(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to i64", v)),
            Convert::Bytes(v) => match v.len() {
                1 => Ok(v[0] as i64),
                4 => Ok(slice_i32(&v) as i64),
                8 => Ok(slice_i64(&v)),
                _ => Err(err!(Code::InternalErr, "{:?} can into i64", v)),
            },
            Convert::BytesRef(v) => match v.len() {
                1 => Ok(v[0] as i64),
                4 => Ok(slice_i32(v) as i64),
                8 => Ok(slice_i64(v)),
                _ => Err(err!(Code::InternalErr, "{:?} can into i64", v)),
            },
            Convert::Bool(v) => {
                if v {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            Convert::U8(v) => Ok(v as i64),
            Convert::I32(v) => Ok(v as i64),
            Convert::U32(v) => Ok(v as i64),
            Convert::I64(v) => Ok(v as i64),
            Convert::U64(v) => {
                if v > 9223372036854775807 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i64`",
                        v
                    ))
                } else {
                    Ok(v as i64)
                }
            }
            Convert::F32(v) => {
                if v < -9223372036854775808.0 || v > 9223372036854775807.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i64`",
                        v
                    ))
                } else {
                    Ok(v as i64)
                }
            }
            Convert::F64(v) => {
                if v < -9223372036854775808.0 || v > 9223372036854775807.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i64`",
                        v
                    ))
                } else {
                    Ok(v as i64)
                }
            }
            Convert::Usize(v) => {
                if v > 9223372036854775807 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `i64`",
                        v
                    ))
                } else {
                    Ok(v as i64)
                }
            }
            Convert::DateTime(v) => Ok(v.timestamp_millis()),
        }
    }
}

impl TryInto<u64> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<u64, Self::Error> {
        match self {
            Convert::String(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to u64", v)),
            Convert::Str(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to u64", v)),
            Convert::Bytes(v) => match v.len() {
                1 => Ok(v[0] as u64),
                4 => Ok(slice_u32(&v) as u64),
                8 => Ok(slice_u64(&v)),
                _ => Err(err!(Code::InternalErr, "{:?} can into u64", v)),
            },
            Convert::BytesRef(v) => match v.len() {
                1 => Ok(v[0] as u64),
                4 => Ok(slice_u32(v) as u64),
                8 => Ok(slice_u64(v)),
                _ => Err(err!(Code::InternalErr, "{:?} can into u64", v)),
            },
            Convert::Bool(v) => {
                if v {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            Convert::U8(v) => Ok(v as u64),
            Convert::I32(v) => Ok(v as u64),
            Convert::U32(v) => Ok(v as u64),
            Convert::I64(v) => Ok(v as u64),
            Convert::U64(v) => Ok(v),
            Convert::F32(v) => {
                if v < 0.0 || v > 18446744073709551615.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u64`",
                        v
                    ))
                } else {
                    Ok(v as u64)
                }
            }
            Convert::F64(v) => {
                if v < 0.0 || v > 18446744073709551615.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u64`",
                        v
                    ))
                } else {
                    Ok(v as u64)
                }
            }
            Convert::Usize(v) => {
                if v > 9223372036854775807 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `u64`",
                        v
                    ))
                } else {
                    Ok(v as u64)
                }
            }
            Convert::DateTime(v) => Ok(v.timestamp_millis() as u64),
        }
    }
}

impl TryInto<f32> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<f32, Self::Error> {
        match self {
            Convert::String(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to f32", v)),
            Convert::Str(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to f32", v)),
            Convert::Bytes(v) => match v.len() {
                1 => Ok(v[0] as f32),
                4 => Ok(slice_f32(&v)),
                8 => {
                    let v = slice_f64(&v);
                    if v < -340282350000000000000000000000000000000.0
                        || v > 340282350000000000000000000000000000000.0
                    {
                        Err(err!(
                            Code::InternalErr,
                            "{:?} literal out of range for `f32`",
                            v
                        ))
                    } else {
                        Ok(v as f32)
                    }
                }
                _ => Err(err!(Code::InternalErr, "{:?} can into f32", v)),
            },
            Convert::BytesRef(v) => match v.len() {
                1 => Ok(v[0] as f32),
                4 => Ok(slice_f32(v)),
                8 => {
                    let v = slice_f64(v);
                    if v < -340282350000000000000000000000000000000.0
                        || v > 340282350000000000000000000000000000000.0
                    {
                        Err(err!(
                            Code::InternalErr,
                            "{:?} literal out of range for `f32`",
                            v
                        ))
                    } else {
                        Ok(v as f32)
                    }
                }
                _ => Err(err!(Code::InternalErr, "{:?} can into f32", v)),
            },
            Convert::Bool(v) => {
                if v {
                    Ok(1.0)
                } else {
                    Ok(0.0)
                }
            }
            Convert::U8(v) => Ok(v as f32),
            Convert::I32(v) => Ok(v as f32),
            Convert::U32(v) => Ok(v as f32),
            Convert::I64(v) => Ok(v as f32),
            Convert::U64(v) => Ok(v as f32),
            Convert::F32(v) => Ok(v),
            Convert::F64(v) => {
                if v < -340282350000000000000000000000000000000.0
                    || v > 340282350000000000000000000000000000000.0
                {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `f32`",
                        v
                    ))
                } else {
                    Ok(v as f32)
                }
            }
            Convert::Usize(v) => {
                if v > 9223372036854775807 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `f32`",
                        v
                    ))
                } else {
                    Ok(v as f32)
                }
            }
            Convert::DateTime(v) => Ok(v.timestamp_millis() as f32),
        }
    }
}

impl TryInto<f64> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<f64, Self::Error> {
        match self {
            Convert::String(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to f64", v)),
            Convert::Str(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to f64", v)),
            Convert::Bytes(v) => match v.len() {
                1 => Ok(v[0] as f64),
                4 => Ok(slice_f32(&v) as f64),
                8 => Ok(slice_f64(&v)),
                _ => Err(err!(Code::InternalErr, "{:?} can into f64", v)),
            },
            Convert::BytesRef(v) => match v.len() {
                1 => Ok(v[0] as f64),
                4 => Ok(slice_f32(v) as f64),
                8 => Ok(slice_f64(v)),
                _ => Err(err!(Code::InternalErr, "{:?} can into f64", v)),
            },
            Convert::Bool(v) => {
                if v {
                    Ok(1.0)
                } else {
                    Ok(0.0)
                }
            }
            Convert::U8(v) => Ok(v as f64),
            Convert::I32(v) => Ok(v as f64),
            Convert::U32(v) => Ok(v as f64),
            Convert::I64(v) => Ok(v as f64),
            Convert::U64(v) => Ok(v as f64),
            Convert::F32(v) => Ok(v as f64),
            Convert::F64(v) => Ok(v),
            Convert::Usize(v) => Ok(v as f64),
            Convert::DateTime(v) => Ok(v.timestamp_millis() as f64),
        }
    }
}

impl TryInto<usize> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<usize, Self::Error> {
        match self {
            Convert::String(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to usize", v)),
            Convert::Str(v) => v
                .parse()
                .map_err(|_| err!(Code::InternalErr, "{} can not convert to usize", v)),
            Convert::Bytes(v) => match v.len() {
                1 => Ok(v[0] as usize),
                4 => Ok(slice_u32(&v) as usize),
                8 => Ok(slice_u64(&v) as usize),
                _ => Err(err!(Code::InternalErr, "{:?} can into usize", v)),
            },
            Convert::BytesRef(v) => match v.len() {
                1 => Ok(v[0] as usize),
                4 => Ok(slice_u32(v) as usize),
                8 => Ok(slice_u64(v) as usize),
                _ => Err(err!(Code::InternalErr, "{:?} can into usize", v)),
            },
            Convert::Bool(v) => {
                if v {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            Convert::U8(v) => Ok(v as usize),
            Convert::I32(v) => Ok(v as usize),
            Convert::U32(v) => Ok(v as usize),
            Convert::I64(v) => Ok(v as usize),
            Convert::U64(v) => Ok(v as usize),
            Convert::F32(v) => {
                if v < 0.0 || v > 18446744073709551615.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `usize`",
                        v
                    ))
                } else {
                    Ok(v as usize)
                }
            }
            Convert::F64(v) => {
                if v < 0.0 || v > 18446744073709551615.0 {
                    Err(err!(
                        Code::InternalErr,
                        "{:?} literal out of range for `usize`",
                        v
                    ))
                } else {
                    Ok(v as usize)
                }
            }
            Convert::Usize(v) => Ok(v),
            Convert::DateTime(v) => Ok(v.timestamp_millis() as usize),
        }
    }
}

impl TryInto<NaiveDateTime> for Convert<'_> {
    type Error = ASError;
    fn try_into(self) -> Result<NaiveDateTime, Self::Error> {
        match self {
            Convert::String(v) => time::format_str(v.as_str()).into(),
            Convert::Str(v) => time::format_str(v).into(),
            Convert::Bytes(v) => Ok(time::from_millis(Convert::from(v).try_into()?)),
            Convert::BytesRef(v) => Ok(time::from_millis(Convert::from(v).try_into()?)),
            Convert::Bool(_v) => result!(Code::InternalErr, "bool can not to datetime"),
            Convert::U8(v) => Ok(time::from_millis(v as i64)),
            Convert::I32(v) => Ok(time::from_millis(v as i64)),
            Convert::U32(v) => Ok(time::from_millis(v as i64)),
            Convert::I64(v) => Ok(time::from_millis(v)),
            Convert::U64(v) => {
                if v > 9223372036854775807 {
                    result!(
                        Code::InternalErr,
                        "{:?} literal out of range for `timestamp`",
                        v
                    )
                } else {
                    Ok(time::from_millis(v as i64))
                }
            }
            Convert::F32(v) => {
                if v < -9223372036854775808.0 || v > 9223372036854775807.0 {
                    result!(
                        Code::InternalErr,
                        "{:?} literal out of range for `timestamp`",
                        v
                    )
                } else {
                    Ok(time::from_millis(v as i64))
                }
            }
            Convert::F64(v) => {
                if v < -9223372036854775808.0 || v > 9223372036854775807.0 {
                    result!(
                        Code::InternalErr,
                        "{:?} literal out of range for `timestamp`",
                        v
                    )
                } else {
                    Ok(time::from_millis(v as i64))
                }
            }
            Convert::Usize(v) => Ok(time::from_millis(v as i64)),
            Convert::DateTime(v) => Ok(v),
        }
    }
}

pub fn json<'a>(v: &'a Value) -> ASResult<Convert<'a>> {
    match v {
        Value::Bool(v) => Ok(Convert::Bool(*v)),
        Value::String(v) => Ok(Convert::Str(v.as_str())),
        Value::Number(v) => {
            if v.is_i64() {
                Ok(Convert::I64(v.as_i64().unwrap()))
            } else if v.is_u64() {
                Ok(Convert::U64(v.as_u64().unwrap()))
            } else {
                Ok(Convert::F64(v.as_f64().unwrap()))
            }
        }
        _ => result!(Code::InternalErr, "{:?} can not convert ", v),
    }
}

#[test]
fn conver_json() {
    use serde_json::json;
    let v: String = json(&json!("123")).unwrap().try_into().unwrap();
    assert_eq!("123", v);
    let v: i64 = json(&json!(123)).unwrap().try_into().unwrap();
    assert_eq!(123, v);
    let v: u64 = json(&json!(123)).unwrap().try_into().unwrap();
    assert_eq!(123, v);
    let v: i32 = json(&json!(123)).unwrap().try_into().unwrap();
    assert_eq!(123, v);
    let v: u32 = json(&json!(123)).unwrap().try_into().unwrap();
    assert_eq!(123, v);
    let v: u8 = json(&json!(123)).unwrap().try_into().unwrap();
    assert_eq!(123, v);
    let v: f64 = json(&json!(123)).unwrap().try_into().unwrap();
    assert_eq!(123.0, v);
    let v: f32 = json(&json!(123)).unwrap().try_into().unwrap();
    assert_eq!(123.0, v);
}

#[test]
fn conver_test() {
    let v: String = Convert::Str("123").try_into().unwrap();
    assert_eq!("123", v);
    let v: u8 = Convert::Str("123").try_into().unwrap();
    assert_eq!(123, v);
    let v: i32 = Convert::Str("123").try_into().unwrap();
    assert_eq!(123, v);
    let v: u32 = Convert::Str("123").try_into().unwrap();
    assert_eq!(123, v);
    let v: i64 = Convert::Str("123").try_into().unwrap();
    assert_eq!(123, v);
    let v: u64 = Convert::Str("123").try_into().unwrap();
    assert_eq!(123, v);
    let v: usize = Convert::Str("123").try_into().unwrap();
    assert_eq!(123, v);
    let v: f32 = Convert::Str("123").try_into().unwrap();
    assert_eq!(123.0, v);
    let v: f64 = Convert::Str("123").try_into().unwrap();
    assert_eq!(123.0, v);
}

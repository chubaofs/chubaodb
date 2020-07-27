// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
use crate::util::error::*;
use crate::*;
use chrono::prelude::*;

pub fn current_millis() -> i64 {
    Local::now().timestamp_millis() as i64
}

pub fn timestamp() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn from_millis(millis: i64) -> NaiveDateTime {
    Utc.timestamp_millis(millis).naive_local()
}

pub fn format_str(s: &str) -> ASResult<NaiveDateTime> {
    match match s.len() {
        19 => NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"),
        10 => match NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            Ok(v) => Ok(v.and_time(NaiveTime::from_num_seconds_from_midnight(0, 0))),
            Err(e) => Err(e),
        },
        // 13 => NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H"),
        16 => NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M"),
        23 => NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"),
        // 25 => NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S% %z"),
        _ => s.parse::<NaiveDateTime>(),
    } {
        Ok(d) => Ok(d.into()),
        Err(e) => result!(
            Code::FieldValueErr,
            "{} can not parse to date, err:{}",
            s,
            e.to_string(),
        ),
    }
}

pub struct Now(NaiveDateTime);

impl Now {
    pub fn new() -> Self {
        Now(Local::now().naive_local())
    }
    pub fn use_time(&self) -> i64 {
        Local::now().timestamp_millis() - self.0.timestamp_millis()
    }

    pub fn use_time_str(&self) -> String {
        format!("{} ms", self.use_time())
    }
}

pub fn i64_time_str(t: i64, format: &str) -> String {
    Utc.timestamp_millis(t).format(format).to_string()
}

pub fn str_time_str<'a>(s: &'a str, format: &str) -> ASResult<String> {
    let timestamp = s
        .parse::<NaiveDateTime>()
        .map_err(|e| {
            ASError::Error(
                Code::FieldValueErr,
                format!("{} can not parse to date, err:{}", s, e.to_string()),
            )
        })?
        .timestamp_millis();
    Ok(i64_time_str(timestamp, format))
}

#[test]
pub fn test_since() {
    let now = Now::new();
    std::thread::sleep(std::time::Duration::from_millis(1200));
    println!("use time : {:?}", now.use_time());
    println!("use time : {:?}", now.use_time_str());
}

#[test]
pub fn test_timestamp() {
    assert_ne!(timestamp(), "2014-11-28 12:00:09");
}

#[test]
pub fn format_str_test() {
    let dt = format_str("2014-11-28 12:00:09").unwrap();
    assert_eq!(1417176009000, dt.timestamp_millis());
    let dt = format_str("2014-11-28 12:00").unwrap();
    assert_eq!(1417176000000, dt.timestamp_millis());
    // let dt = format_str("2014-11-28 12").unwrap();
    // assert_eq!(1417176000000, dt.timestamp_millis());
    let dt = format_str("2014-11-28").unwrap();
    assert_eq!(1417132800000, dt.timestamp_millis());

    let dt = format_str("2014-11-28 12:00:09.123").unwrap();
    assert_eq!(1417176009123, dt.timestamp_millis());

    // let dt = format_str("2014-11-28 12:00:09+09:30").unwrap();
    // assert_eq!(1417176009123, dt.timestamp_millis());
}

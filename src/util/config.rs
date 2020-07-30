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
use crate::util::net::MyIp;
use git_version::git_version;
use log::{info, LevelFilter};
use log4rs::{
    append::{
        console::{ConsoleAppender, Target},
        rolling_file::{
            policy::compound::{
                roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
            },
            RollingFileAppender,
        },
    },
    config::{Appender, Config as LogConfig, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};
use serde_derive::Deserialize;
use std::fs::File;
use std::io::prelude::*;
use toml;

pub const VERSION: &str = clap::crate_version!();
pub const GIT_VERSION: &str = git_version!();

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub global: Global,
    pub router: Router,
    pub ps: PS,
    pub masters: Vec<Master>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Global {
    pub name: String,
    #[serde(default = "empty_str")]
    pub ip: String,
    pub log: String,
    pub log_level: String,
    #[serde(default = "default_log_limit_bytes")]
    pub log_limit_bytes: usize,
    #[serde(default = "default_log_file_count")]
    pub log_file_count: usize,
    pub shared_disk: bool,
}

fn default_log_limit_bytes() -> usize {
    128 * 1024 * 1024
}

fn default_log_file_count() -> usize {
    100
}

#[derive(Debug, Deserialize, Clone)]
pub struct Router {
    pub http_port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PS {
    // id value not need set in config, It will be assigned by the master
    pub id: Option<u64>,
    pub zone: String,
    pub data: String,
    pub rpc_port: u16,
    pub flush_sleep_sec: Option<u64>,
    pub raft: RaftConf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RaftConf {
    pub heartbeat_port: u16,
    pub replicate_port: u16,
    // how size of num for memory
    pub log_max_num: usize,
    // how size of num for memory
    pub log_min_num: usize,
    // how size of num for memory
    pub log_file_size_mb: u64,
    //Three  without a heartbeat , follower to begin consecutive elections
    pub heartbeate_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Master {
    pub node_id: u64,
    pub ip: String,
    pub http_port: u16,
    #[serde(default = "false_bool")]
    pub is_self: bool,
    pub data: String,
    pub raft: RaftConf,
}

impl Config {
    //init once for starup
    fn init(&mut self) {
        if self.global.ip == "" {
            let my = MyIp::instance().unwrap();
            for m in self.masters.iter_mut() {
                if my.is_my_ip(m.ip.as_str()) {
                    m.is_self = true;
                    break;
                }
            }
        } else {
            for m in self.masters.iter_mut() {
                if m.ip.as_str() == self.global.ip.as_str() {
                    m.is_self = true;
                    break;
                }
            }
        }

        // init log in
        let level = match self.global.log_level.to_uppercase().as_str() {
            "DEBUG" => {
                std::env::set_var("RUST_LOG", "actix_web=debug"); //FIXME: not worked
                LevelFilter::Debug
            }
            "INFO" => LevelFilter::Info,
            "WARN" => LevelFilter::Warn,
            "TRACE" => LevelFilter::Trace,
            "ERROR" => LevelFilter::Error,
            "OFF" => LevelFilter::Off,
            _ => panic!("can not find log level:{}", self.global.log_level),
        };

        let stdout = ConsoleAppender::builder()
            .target(Target::Stdout)
            .encoder(Box::new(PatternEncoder::new(
                "{d(%Y-%m-%d %H:%M:%S)} - {l} - {t}\\({L}\\) - {m}{n}",
            )))
            .build();

        let chubaodb = RollingFileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(
                "{d(%Y-%m-%d %H:%M:%S)} - {l} - {t}\\({L}\\) - {m}{n}",
            )))
            .build(
                std::path::Path::new(self.global.log.as_str()).join("chubaodb.log"),
                Box::new(CompoundPolicy::new(
                    Box::new(SizeTrigger::new(1024 * 1024 * 128)),
                    Box::new(
                        FixedWindowRoller::builder()
                            .build(
                                std::path::Path::new(self.global.log.as_str())
                                    .join("chubaodb.{}.log")
                                    .to_str()
                                    .unwrap(),
                                2000,
                            )
                            .unwrap(),
                    ),
                )),
            )
            .unwrap();

        let config = LogConfig::builder()
            .appender(Appender::builder().build("chubaodb", Box::new(chubaodb)))
            .appender(
                Appender::builder()
                    .filter(Box::new(ThresholdFilter::new(level)))
                    .build("stdout", Box::new(stdout)),
            )
            .build(
                Root::builder()
                    .appender("chubaodb")
                    .appender("stdout")
                    .build(level),
            )
            .unwrap();

        let _handle = log4rs::init_config(config).expect("init log config has err");

        info!("log init ok ");
    }

    pub fn self_master(&self) -> Option<&Master> {
        for m in self.masters.iter() {
            if m.is_self {
                return Some(m);
            }
        }
        None
    }

    pub fn master_addr(&self) -> String {
        return format!("{}:{}", self.masters[0].ip, self.masters[0].http_port);
    }

    pub fn master_http_addr(&self) -> String {
        format!(
            "http://{}:{}",
            self.masters[0].ip, self.masters[0].http_port
        )
    }
}

pub fn load_config(conf_path: &str, ip: Option<&str>) -> Config {
    let mut config = _load_config(conf_path, ip);
    config.init();
    config
}

fn _load_config(conf_path: &str, ip: Option<&str>) -> Config {
    if conf_path == "default" {
        return Config {
            global: Global {
                name: String::from("chubaodb"),
                ip: String::from("127.0.0.1"),
                log: String::from("log/"),
                log_level: String::from("info"),
                log_limit_bytes: default_log_limit_bytes(),
                log_file_count: default_log_file_count(),
                shared_disk: true,
            },
            ps: PS {
                id: None,
                zone: String::from("default"),
                data: String::from("data/ps"),
                rpc_port: 9090,
                flush_sleep_sec: Some(3),
                raft: RaftConf {
                    heartbeat_port: 12130,
                    replicate_port: 12131,
                    log_max_num: 20000,
                    log_min_num: 10000,
                    log_file_size_mb: 32,
                    heartbeate_ms: 500,
                },
            },
            router: Router { http_port: 8080 },
            masters: vec![Master {
                node_id: 1,
                ip: String::from("127.0.0.1"),
                http_port: 7070,
                is_self: true,
                data: String::from("data/"),
                raft: RaftConf {
                    heartbeat_port: 22130,
                    replicate_port: 22131,
                    log_max_num: 20000,
                    log_min_num: 10000,
                    log_file_size_mb: 32,
                    heartbeate_ms: 500,
                },
            }],
        };
    }

    let mut file = match File::open(conf_path) {
        Ok(f) => f,
        Err(e) => panic!("no such file {} exception:{}", conf_path, e),
    };
    let mut str_val = String::new();

    match file.read_to_string(&mut str_val) {
        Ok(s) => s,
        Err(e) => panic!("Error Reading file: {}", e),
    };
    let mut config: Config = toml::from_str(&str_val).unwrap();

    if let Some(ip) = ip {
        config.global.ip = ip.to_string();
    }

    return config;
}

fn empty_str() -> String {
    "".to_string()
}

fn false_bool() -> bool {
    false
}

#[test]
fn test_load_config() {
    let config = load_config("config/config.toml", None);
    assert_eq!(1, config.self_master().unwrap().node_id);
}

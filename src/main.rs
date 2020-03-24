// Copyright 2020 The Chubao Authors. Licensed under Apache-2.0.

use chubaodb::{master, pserver, router, util};
use clap::{App, Arg, SubCommand};
use log::{error, info};
use std::sync::{mpsc::channel, Arc};
use std::thread;

fn main() {
    let app = App::new("anyindex")
        .version("0.1.0")
        .about("hello index world")
        .subcommand(
            SubCommand::with_name("all")
                .arg(
                    Arg::with_name("ip")
                        .short("i")
                        .value_name("IP")
                        .required(false)
                        .default_value("")
                        .help("set your local ip , otherwise it will be automatically discovered"),
                )
                .arg(
                    Arg::with_name("config")
                        .short("c")
                        .value_name("CONFIG")
                        .required(true)
                        .help("set your config file path, or as 'default'"),
                ),
        )
        .subcommand(
            SubCommand::with_name("master")
                .arg(
                    Arg::with_name("ip")
                        .short("i")
                        .value_name("IP")
                        .required(false)
                        .default_value("")
                        .help("set your local ip, otherwise it will be automatically discovered"),
                )
                .arg(
                    Arg::with_name("config")
                        .short("c")
                        .value_name("CONFIG")
                        .required(true)
                        .help("set your config file path, or as 'default'"),
                ),
        )
        .subcommand(
            SubCommand::with_name("ps").arg(
                Arg::with_name("config")
                    .short("c")
                    .value_name("CONFIG")
                    .required(true)
                    .help("set your config file path, or as 'default'"),
            ),
        )
        .subcommand(
            SubCommand::with_name("router").arg(
                Arg::with_name("config")
                    .short("c")
                    .long("config")
                    .value_name("CONFIG")
                    .default_value("config/config.toml")
                    .help("set your config file path, or as 'default'"),
            ),
        )
        .get_matches();

    let (mut subcommand, some_options) = app.subcommand();

    let conf = match some_options {
        Some(so) => {
            let conf_path = so
                .value_of("config")
                .or_else(|| panic!("No config file path"))
                .unwrap();
            println!("load config by path: {}", conf_path);
            util::config::load_config(conf_path, so.value_of("ip"))
        }
        None => {
            eprintln!("No config args were found so load the default values!");
            subcommand = "all";
            let mut conf = util::config::load_config("default", None);
            conf.global.ip = String::from("127.0.0.1");
            conf
        }
    };

    let arc_conf = Arc::new(conf);

    let (tx, rx) = channel::<String>();

    match subcommand {
        "master" => {
            let arc = arc_conf.clone();
            let tx_clone = tx.clone();
            thread::spawn(|| {
                let _ = master::server::start(tx_clone, arc);
            });
        }
        "ps" => {
            let arc = arc_conf.clone();
            let tx_clone = tx.clone();
            thread::spawn(|| {
                let _ = pserver::server::start(tx_clone, arc);
            });
        }
        "router" => {
            let arc = arc_conf.clone();
            let tx_clone = tx.clone();
            thread::spawn(|| {
                let _ = router::server::start(tx_clone, arc).unwrap();
            });
        }
        "all" => {
            let arc = arc_conf.clone();
            let tx_clone = tx.clone();
            thread::spawn(|| {
                let _ = master::server::start(tx_clone, arc);
            });

            let arc = arc_conf.clone();
            let tx_clone = tx.clone();
            thread::spawn(|| {
                let _ = router::server::start(tx_clone, arc).unwrap();
            });

            let arc = arc_conf.clone();
            let tx_clone = tx.clone();
            thread::spawn(|| {
                let _ = pserver::server::start(tx_clone, arc);
            });
        }
        _ => panic!("Subcommand {} is unknow", subcommand),
    }

    info!("All ChubaoDB servers were started successfully!");

    error!("{:?}", rx.recv().unwrap());
}

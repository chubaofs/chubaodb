use crate::util::error::*;
use crate::*;
use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;

const STOP_CODE: u8 = 100;
const START_PORT: u32 = 30000;

pub struct MyIp {
    port: u32,
    token: u64,
}

impl Drop for MyIp {
    fn drop(&mut self) {
        if let Ok(mut socket) = TcpStream::connect(format!("0.0.0.0:{}", self.port)) {
            socket.write(&[STOP_CODE]).unwrap();
        };
    }
}

impl MyIp {
    pub fn instance() -> ASResult<MyIp> {
        let rnd = rand::random::<u32>();

        let mut port = START_PORT + rnd % START_PORT;
        let token = rand::random::<u64>();

        loop {
            if let Ok(listener) = TcpListener::bind(format!("0.0.0.0:{}", port)) {
                std::thread::spawn(move || loop {
                    for stream in listener.incoming() {
                        let mut stream = stream.unwrap();
                        let mut buffer = [0; 1];
                        let size = stream.read(&mut buffer).unwrap();
                        if size > 1 {
                            continue;
                        }
                        if buffer[0] == STOP_CODE {
                            break;
                        }
                        stream.write(&token.to_be_bytes()).unwrap();
                    }
                });
                return Ok(MyIp {
                    port: port,
                    token: token,
                });
            }

            port = port + 1;
            if port >= 65535 {
                break;
            }
        }

        return result_def!("no port can use");
    }

    //Determine if an IP is native
    pub fn is_my_ip(&self, ip: &str) -> bool {
        if let Ok(mut socket) = TcpStream::connect(format!("{}:{}", ip, self.port)) {
            socket.write(&[1]).unwrap();
            let mut result = Vec::with_capacity(8);
            socket.read_to_end(&mut result).unwrap();
            let mut v: [u8; 8] = Default::default();
            v.copy_from_slice(&result);
            return u64::from_be_bytes(v) == self.token;
        }
        return false;
    }
}

#[test]
fn my_ip_test() {
    let my_ip = MyIp::instance().unwrap();
    assert!(my_ip.is_my_ip("127.0.0.1"));
    assert!(!my_ip.is_my_ip("255.255.255.255"));
}

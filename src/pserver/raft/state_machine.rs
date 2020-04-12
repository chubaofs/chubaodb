use crate::pserver::simba::simba::Simba;
use crate::util::{coding::*, error::*};
use jimraft::{error::RResult, CmdResult, ConfigChange, Snapshot, StateMachine};
use log::error;
use std::boxed::Box;
use std::sync::Mutex;
use std::sync::{mpsc::Sender, Arc};

//collection_id, partition_id, leader_id
#[derive(Debug)]
pub struct MemberChange(pub u32, pub u32, pub u64);

pub struct SimpleStateMachine {
    pub persisted: u64,
    pub peer_id: u64,
    pub collection_id: u32,
    pub partition_id: u32,
    pub sender: Arc<Mutex<Sender<MemberChange>>>,
}
use libc::{self, c_void};
impl StateMachine for SimpleStateMachine {
    fn apply(&mut self, result: &CmdResult) -> RResult<()> {
        self.persisted = result.index;
        unsafe {
            if result.tag != 0 as *mut c_void {
                let cb = &mut *(result.tag as *mut AppendCallbackFaced);
                cb.call(result);
            }
        }
        Ok(())
    }

    fn apply_member_change(&self, _conf: *const ConfigChange, _member: u64) -> RResult<()> {
        Ok(())
    }

    fn persist_applied(&self) -> RResult<u64> {
        Ok(self.persisted)
    }

    fn on_leader_change(&self, leader: u64, _term: u64) {
        if let Err(e) = self.sender.lock().unwrap().send(MemberChange(
            self.collection_id,
            self.partition_id,
            leader,
        )) {
            error!(
                "collection_id:{} partition_id:{} , send change member has err:{:?}",
                self.collection_id, self.partition_id, e
            );
        };
    }

    fn get_snapshot(&self) -> RResult<Snapshot> {
        unimplemented!()
    }

    fn apply_snapshot_start(&self, _context: Vec<u8>, _index: u64) -> RResult<()> {
        Ok(())
    }

    fn apply_snapshot_data(&self, _datas: Vec<Vec<u8>>) -> RResult<()> {
        Ok(())
    }

    fn apply_snapshot_finish(&mut self, _index: u64) -> RResult<()> {
        Ok(())
    }

    fn apply_read_index(&self, _cmd: Vec<u8>, _index: u16) -> RResult<()> {
        Ok(())
    }
}

pub trait AppendCallback {
    fn call(&self, result: &CmdResult);
}

pub struct AppendCallbackFaced {
    pub target: Box<dyn AppendCallback>,
}

impl AppendCallbackFaced {
    pub fn call(&self, result: &CmdResult) {
        self.target.call(result);
    }
}

#[derive(PartialEq, Copy, Clone)]
pub enum EventType {
    Delete = 0,
    Create = 1,
    Update = 2,
}

pub enum Event {
    //key+ old_id+ 0
    Delete(Vec<u8>, Vec<u8>),
    //value+ key + len(key)+1
    Create(Vec<u8>, Vec<u8>),
    //value + key + len(k) + iid + 2
    Update(Vec<u8>, Vec<u8>, Vec<u8>),
}

pub struct EventCodec {}

impl EventCodec {
    pub fn encode(event: Event) -> Vec<u8> {
        match event {
            Event::Delete(iid, mut k) => {
                let need_len = k.len() + iid.len() + 1;
                if k.capacity() < need_len {
                    k.reserve(need_len - k.capacity());
                }
                k.extend_from_slice(&iid);
                k.push(EventType::Delete as u8);
                k
            }
            Event::Create(k, mut v) => {
                let need_len = v.len() + k.len() + 3;
                if v.capacity() < need_len {
                    v.reserve(need_len - v.capacity());
                }
                v.extend_from_slice(&k);
                v.extend_from_slice(&u16_slice(k.len() as u16)[..]);
                v.push(EventType::Create as u8);
                v
            }
            Event::Update(iid, k, mut v) => {
                let need_len = v.len() + k.len() + 11;
                if v.capacity() < need_len {
                    v.reserve(need_len - v.capacity());
                }
                v.extend_from_slice(&k);
                v.extend_from_slice(&u16_slice(k.len() as u16)[..]);
                v.extend_from_slice(&iid);
                v.push(EventType::Update as u8);
                v
            }
        }
    }

    //iid  key value
    pub fn decode<'a>(data: &'a Vec<u8>) -> (EventType, i64, &'a [u8], &'a [u8]) {
        let empty: &'static [u8] = &[0];
        //key+ old_id+ 0
        //value+ key + len(key)+1
        //value + key + len(k) + iid + 2
        let len = data.len() - 1;
        match data[len] {
            0 => (
                EventType::Delete,
                slice_i64(&data[len - 8..len]),
                &data[..len - 8],
                empty,
            ),
            1 => {
                let key_len = slice_u16(&data[len - 2..len]) as usize;
                (
                    EventType::Create,
                    0,
                    &data[len - 2 - key_len..len - 2],
                    &data[..len - 2 - key_len],
                )
            }
            2 => {
                let key_len = slice_u16(&data[len - 10..len - 8]) as usize;
                (
                    EventType::Update,
                    slice_i64(&data[len - 8..len]),
                    &data[len - key_len - 10..len - 10],
                    &data[..len - key_len - 10],
                )
            }
            _ => panic!("decode has err type:{}", data[len]),
        }
    }
}

pub struct WriteRaftCallback {
    simba: Arc<Simba>,
    tx: Sender<GenericError>,
}

impl WriteRaftCallback {
    pub fn new(tx: Sender<GenericError>, simba: Arc<Simba>) -> WriteRaftCallback {
        WriteRaftCallback {
            tx: tx,
            simba: simba,
        }
    }

    fn send_result(&self, ge: GenericError) {
        if let Err(e) = self.tx.send(ge) {
            error!("write result has err:{:?}", e); //TODO: if errr
        };
    }
}

impl AppendCallback for WriteRaftCallback {
    fn call(&self, cmd: &CmdResult) {
        if cmd.rep_status.code > 0 {
            self.send_result(GenericError(
                INTERNAL_ERR,
                format!("send raft has code:{}", cmd.rep_status.code),
            ));
            return;
        };

        if let Err(e) = self.simba.do_write(cmd.index, &cmd.data, false) {
            self.send_result(err(e.to_string()));
        } else {
            return self.send_result(GenericError(SUCCESS, String::default()));
        }
    }
}

#[test]
fn test_EventCodec() {
    use crate::util::coding::i64_slice;
    let key = &String::from("test_key")[..];
    let value = &String::from("test_value")[..];
    let iid = i64_slice(123)[..];
    let event = Event::Create(key, value);
    let value = EventCodec::encode(event);
    println!("{:?}", value);
}

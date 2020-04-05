use crate::util::entity::Partition;
use crate::util::{coding::*, config, entity::*, error::*};
use jimraft::{
    error::RResult, raft::LogReader, CmdResult, ConfigChange, NodeResolver, NodeResolverCallback,
    Peer, PeerType, Raft, RaftOptions, RaftServer, RaftServerOptions, Snapshot, StateMachine,
    StateMachineCallback,
};

use crate::client::meta_client::MetaClient;
use std::boxed::Box;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::runtime::Builder;

pub struct SimpleStateMachine {
    pub persisted: u64,
    pub peer_id: u64,
}

impl StateMachine for SimpleStateMachine {
    fn apply(&mut self, result: &CmdResult) -> RResult<()> {
        self.persisted = result.index;
        unsafe {
            let cb = &mut *(result.tag as *mut AppendCallbackFaced);
            cb.call(result.index);
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
        //TODO may be some error found
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
    fn call(&self, persist_index: u64);
}

pub struct AppendCallbackFaced {
    pub target: Box<dyn AppendCallback>,
}

impl AppendCallbackFaced {
    pub fn call(&self, persist_index: u64) {
        self.target.call(persist_index);
    }
}

pub enum EventType {
    Put = 1,
    Delete = 2,
}

pub enum Event {
    Delete(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
}

pub struct EventCodec {}

impl EventCodec {
    pub fn encode(event: Event) -> Vec<u8> {
        let mut result = vec![];
        match event {
            Event::Delete(mut k) => {
                result.push(EventType::Delete as u8);
                result.append(u32_slice(k.len() as u32).to_vec().as_mut());
                result.append(k.as_mut());
            }
            Event::Put(mut k, mut v) => {
                result.push(EventType::Put as u8);
                result.append(u32_slice(k.len() as u32).to_vec().as_mut());
                result.append(k.as_mut());
                result.append(u32_slice(v.len() as u32).to_vec().as_mut());
                result.append(v.as_mut());
            }
        }
        result
    }

    pub fn decode(data: Vec<u8>) -> ASResult<Event> {
        let (event_type, payload) = data.split_at(1);
        if event_type[0] == EventType::Delete as u8 {
            let (_k_len, key) = payload.split_at(4);
            return Ok(Event::Delete(key.to_vec()));
        } else if event_type[0] == EventType::Put as u8 {
            let (k_len, right) = payload.split_at(4);
            let (k, right) = right.split_at(slice_u32(k_len) as usize);
            let (_v_len, v) = right.split_at(4);
            return Ok(Event::Put(k.to_vec(), v.to_vec()));
        } else {
            return Err(err_str_box("unrecognized log event"));
        }
    }
}

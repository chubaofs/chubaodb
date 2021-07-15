pub mod key;
pub mod network;

use alaya_protocol::raft::{write_action, Entry as RaftEntry, WriteActions};
use anyhow::Result;
use prost::Message;
use rocksdb::{Direction, IteratorMode, ReadOptions, WriteBatch, DB};
use serde::de::DeserializeOwned;
use serde::Deserializer;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use xraft::{
    Entry, EntryDetail, HardState, InitialState, LogIndex, MemberShipConfig, NodeId, Storage,
    StorageResult,
};

pub const CF_LOG: &str = "log";
pub const CF_DATA: &str = "data";

pub struct RaftStorage {
    db: Arc<DB>,
    scope: Vec<u8>,
    membership: RwLock<Option<MemberShipConfig<NodeId>>>,
}

impl RaftStorage {
    pub fn new(db: Arc<DB>, scope: Option<&[u8]>) -> Self {
        Self {
            db,
            scope: scope.map(|data| data.to_vec()).unwrap_or_default(),
            membership: RwLock::new(Some(MemberShipConfig::default())),
        }
    }

    pub fn exists<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        let cf_data = self.db.cf_handle(CF_DATA).unwrap();
        Ok(self
            .db
            .get_cf(cf_data, key::data(&self.scope, key.as_ref()))?
            .is_some())
    }

    pub fn get<K: AsRef<[u8]>, V: Message + Default>(&self, key: K) -> Result<Option<V>> {
        let cf_data = self.db.cf_handle(CF_DATA).unwrap();
        match self
            .db
            .get_cf(cf_data, key::data(&self.scope, key.as_ref()))?
        {
            Some(value) => Ok(Some(V::decode(&*value)?)),
            None => Ok(None),
        }
    }

    pub fn get_raw<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        let cf_data = self.db.cf_handle(CF_DATA).unwrap();
        match self
            .db
            .get_cf(cf_data, key::data(&self.scope, key.as_ref()))?
        {
            Some(value) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        let cf_data = self.db.cf_handle(CF_DATA).unwrap();
        Ok(self
            .db
            .delete_cf(cf_data, key::data(&self.scope, key.as_ref()))?)
    }

    pub fn delete_scope(&self, scope: impl AsRef<[u8]>) -> Result<()> {
        let cf_data = self.db.cf_handle(CF_DATA).unwrap();
        let cf_log = self.db.cf_handle(CF_LOG).unwrap();
        let mut wb = WriteBatch::default();
        wb.delete_range_cf(
            cf_log,
            key::scope_start(scope.as_ref()),
            key::scope_end(scope.as_ref()),
        );
        wb.delete_range_cf(
            cf_data,
            key::scope_start(scope.as_ref()),
            key::scope_end(scope.as_ref()),
        );
        Ok(self.db.write(wb)?)
    }

    pub fn iterator<'a, S: AsRef<[u8]>, E: AsRef<[u8]>, V: Message + Default>(
        &'a self,
        start: S,
        end: E,
    ) -> impl Iterator<Item = (Vec<u8>, V)> + 'a {
        let mut opts = ReadOptions::default();
        let cf_data = self.db.cf_handle(CF_DATA).unwrap();
        opts.set_iterate_upper_bound(&*key::data(&self.scope, end.as_ref()));
        self.db
            .iterator_cf_opt(
                cf_data,
                opts,
                IteratorMode::From(&key::data(&self.scope, start.as_ref()), Direction::Forward),
            )
            .filter_map(move |(key, value)| {
                let key = match key::decode_data(&self.scope, &key) {
                    Some(key) => key.to_vec(),
                    None => return None,
                };
                let value = match V::decode(&*value) {
                    Ok(value) => value,
                    Err(_) => return None,
                };
                Some((key, value))
            })
    }

    pub fn iterator_raw<'a, S: AsRef<[u8]>, E: AsRef<[u8]>>(
        &'a self,
        start: S,
        end: E,
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a {
        let mut opts = ReadOptions::default();
        let cf_data = self.db.cf_handle(CF_DATA).unwrap();
        opts.set_iterate_upper_bound(&*key::data(&self.scope, end.as_ref()));
        self.db
            .iterator_cf_opt(
                cf_data,
                opts,
                IteratorMode::From(&key::data(&self.scope, start.as_ref()), Direction::Forward),
            )
            .filter_map(move |(key, value)| {
                let key = match key::decode_data(&self.scope, &key) {
                    Some(key) => key.to_vec(),
                    None => return None,
                };
                Some((key, value.to_vec()))
            })
    }

    fn hard_state(&self) -> Result<Option<HardState>> {
        let cf_log = self.db.cf_handle(CF_LOG).unwrap();
        match self.db.get_cf(cf_log, key::hard_state(&self.scope))? {
            Some(value) => Ok(Some(bincode::deserialize(&value)?)),
            None => Ok(None),
        }
    }

    fn last_entry(&self) -> Result<Option<RaftEntry>> {
        let cf_log = self.db.cf_handle(CF_LOG).unwrap();
        match self
            .db
            .iterator_cf(
                cf_log,
                IteratorMode::From(&key::entry(&self.scope, u64::MAX), Direction::Reverse),
            )
            .next()
        {
            Some((key, value)) => {
                if !key::is_entry(&self.scope, &key) {
                    Ok(None)
                } else {
                    Ok(Some(bincode::deserialize(&*value)?))
                }
            }
            None => Ok(None),
        }
    }
}

impl Storage<NodeId, WriteActions> for RaftStorage {
    fn get_initial_state(&self) -> StorageResult<InitialState<NodeId>> {
        let membership = self.membership.read().unwrap();
        let hard_state = self.hard_state()?;
        let (last_log_index, last_log_term) = match self.last_entry()? {
            Some(entry) => (entry.index, entry.term),
            None => (0, 0),
        };
        Ok(InitialState {
            last_log_index,
            last_log_term,
            hard_state,
            membership: membership.clone(),
        })
    }

    fn save_hard_state(&self, hard_state: HardState) -> Result<()> {
        let cf_log = self.db.cf_handle(CF_LOG).unwrap();
        Ok(self.db.put_cf(
            cf_log,
            key::hard_state(&self.scope),
            bincode::serialize(&hard_state)?,
        )?)
    }

    fn last_applied(&self) -> Result<u64> {
        let cf_log = self.db.cf_handle(CF_LOG).unwrap();
        match self.db.get_cf(cf_log, key::last_applied_log(&self.scope))? {
            Some(value) => Ok(bincode::deserialize(&value)?),
            None => Ok(0),
        }
    }

    fn get_log_entries(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> StorageResult<Vec<Entry<NodeId, WriteActions>>> {
        let mut entries = Vec::new();
        let end = key::entry(&self.scope, end);
        let cf_log = self.db.cf_handle(CF_LOG).unwrap();
        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(&*end);
        for (_, value) in self.db.iterator_cf_opt(
            cf_log,
            opts,
            IteratorMode::From(&key::entry(&self.scope, start), Direction::Forward),
        ) {
            entries.push(bincode::deserialize(&*value).unwrap());
        }
        Ok(entries)
    }

    fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        let cf_log = self.db.cf_handle(CF_LOG).unwrap();
        self.db.delete_range_cf(
            cf_log,
            key::entry(&self.scope, start),
            match stop {
                Some(stop) => key::entry(&self.scope, stop),
                None => key::entry_end(&self.scope),
            },
        )?;
        Ok(())
    }

    fn append_entries_to_log(&self, entries: &[Entry<NodeId, WriteActions>]) -> StorageResult<()> {
        let cf_log = self.db.cf_handle(CF_LOG).unwrap();
        let mut wb = WriteBatch::default();
        for entry in entries {
            wb.put_cf(
                cf_log,
                key::entry(&self.scope, entry.index),
                bincode::serialize(entry).unwrap(),
            );
        }
        self.db.write(wb)?;
        Ok(())
    }

    fn apply_entries_to_state_machine(
        &self,
        entries: &[Entry<NodeId, WriteActions>],
    ) -> StorageResult<()> {
        let cf_log = self.db.cf_handle(CF_LOG).unwrap();
        let cf_data = self.db.cf_handle(CF_DATA).unwrap();

        let last_index = entries.last().unwrap().index;
        let mut wb = WriteBatch::default();

        for entry in entries {
            if let EntryDetail::Normal(actions) = &entry.detail {
                for action in actions.actions.iter() {
                    let ty = action.r#type();
                    let kv = action.kv.as_ref().unwrap();
                    match ty {
                        write_action::Type::Put => {
                            wb.put_cf(cf_data, key::data(&self.scope, &kv.key), &kv.value);
                        }
                        write_action::Type::Delete => {
                            wb.delete_cf(cf_data, key::data(&self.scope, &kv.key));
                        }
                    }
                }
            }
        }
        wb.put_cf(
            cf_log,
            key::last_applied_log(&self.scope),
            bincode::serialize(&last_index)?,
        );
        self.db.write(wb)?;
        Ok(())
    }
}

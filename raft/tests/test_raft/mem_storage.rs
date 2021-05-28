use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use xraft::{
    Entry, EntryDetail, HardState, InitialState, LogIndex, MemberShipConfig, Storage, StorageResult,
};

use super::Action;

#[derive(Default)]
struct Inner {
    hard_state: Option<HardState>,
    membership: Option<MemberShipConfig<()>>,
    logs: BTreeMap<LogIndex, Entry<(), Action>>,
    last_applied: LogIndex,
    kvs: HashMap<String, i32>,
}

#[derive(Default)]
pub struct MemoryStorage {
    inner: RwLock<Inner>,
}

impl MemoryStorage {
    pub fn get(&self, key: impl AsRef<str>) -> Option<i32> {
        self.inner.read().unwrap().kvs.get(key.as_ref()).copied()
    }
}

impl Storage<(), Action> for MemoryStorage {
    fn get_initial_state(&self) -> StorageResult<InitialState<()>> {
        let inner = self.inner.read().unwrap();
        let last_entry = inner.logs.iter().rev().next().map(|(_, entry)| entry);
        Ok(InitialState {
            last_log_index: last_entry.map(|entry| entry.index).unwrap_or_default(),
            last_log_term: last_entry.map(|entry| entry.term).unwrap_or_default(),
            hard_state: inner.hard_state,
            membership: inner.membership.clone(),
        })
    }

    fn save_hard_state(&self, hard_state: HardState) -> StorageResult<()> {
        self.inner.write().unwrap().hard_state = Some(hard_state);
        Ok(())
    }

    fn last_applied(&self) -> StorageResult<u64> {
        Ok(self.inner.read().unwrap().last_applied)
    }

    fn get_log_entries(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> StorageResult<Vec<Entry<(), Action>>> {
        Ok(self
            .inner
            .read()
            .unwrap()
            .logs
            .range(start..end)
            .map(|(_, entry)| Entry::clone(entry))
            .collect())
    }

    fn delete_logs_from(&self, start: LogIndex, end: Option<LogIndex>) -> StorageResult<()> {
        let mut inner = self.inner.write().unwrap();
        let range = match end {
            Some(end) => inner.logs.range(start..end),
            None => inner.logs.range(start..),
        }
        .map(|(idx, _)| *idx)
        .collect::<Vec<_>>();
        for idx in range {
            inner.logs.remove(&idx);
        }
        Ok(())
    }

    fn append_entries_to_log(&self, entries: &[Entry<(), Action>]) -> StorageResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner
            .logs
            .extend(entries.iter().map(|entry| (entry.index, entry.clone())));
        Ok(())
    }

    fn apply_entries_to_state_machine(&self, entries: &[Entry<(), Action>]) -> StorageResult<()> {
        let mut inner = self.inner.write().unwrap();
        for entry in entries {
            if let EntryDetail::Normal(action) = &entry.detail {
                match action {
                    Action::Put(name, value) => {
                        inner.kvs.insert(name.clone(), *value);
                    }
                    Action::Delete(name) => {
                        inner.kvs.remove(name);
                    }
                }
            }
        }
        inner.last_applied = entries.last().unwrap().index;
        Ok(())
    }
}

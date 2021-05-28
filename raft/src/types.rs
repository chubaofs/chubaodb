use std::fmt::Formatter;
use std::sync::Arc;

use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};
use smallvec::alloc::fmt::Display;

use crate::{RaftError, Result};

pub type NodeId = u64;
pub type TermId = u64;
pub type LogIndex = u64;

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    NonVoter,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemberShipConfig<N> {
    pub members: FnvHashMap<NodeId, Arc<N>>,
    pub non_voters: FnvHashMap<NodeId, Arc<N>>,
}

impl<N> Display for MemberShipConfig<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberShipConfig")
            .field("members", &self.members.keys())
            .field("non_voters", &self.non_voters.keys())
            .finish()
    }
}

impl<N> Clone for MemberShipConfig<N> {
    fn clone(&self) -> Self {
        Self {
            members: self.members.clone(),
            non_voters: self.non_voters.clone(),
        }
    }
}

impl<N> Default for MemberShipConfig<N> {
    fn default() -> Self {
        Self {
            members: Default::default(),
            non_voters: Default::default(),
        }
    }
}

impl<N> MemberShipConfig<N> {
    pub(crate) fn other_members(&self, current: NodeId) -> impl Iterator<Item = (NodeId, Arc<N>)> {
        self.other_voter_members(current)
            .chain(self.non_voters.clone())
    }

    pub(crate) fn other_voter_members(
        &self,
        current: NodeId,
    ) -> impl Iterator<Item = (NodeId, Arc<N>)> {
        self.members
            .clone()
            .into_iter()
            .filter(move |(id, _)| *id != current)
    }

    pub(crate) fn member_info(&self, id: NodeId) -> Option<Arc<N>> {
        self.members
            .get(&id)
            .or_else(|| self.non_voters.get(&id))
            .cloned()
    }

    #[inline]
    pub(crate) fn is_non_voter(&self, id: NodeId) -> bool {
        self.non_voters.contains_key(&id)
    }

    #[inline]
    pub(crate) fn is_member(&self, id: NodeId) -> bool {
        self.members.contains_key(&id)
    }

    pub(crate) fn convert_voter_to_follower(&self, id: NodeId) -> Self {
        assert!(!self.members.contains_key(&id) && self.non_voters.contains_key(&id));
        let mut new_members = self.members.clone();
        let mut new_non_voters = self.non_voters.clone();
        new_members.extend(new_non_voters.remove(&id).map(|info| (id, info)));
        Self {
            members: new_members,
            non_voters: new_non_voters,
        }
    }

    pub(crate) fn add_non_voter(&self, id: NodeId, info: N) -> Result<Self> {
        if self.is_member(id) || self.is_non_voter(id) {
            return Err(RaftError::NodeAlreadyRegistered(id));
        }
        Ok(Self {
            members: self.members.clone(),
            non_voters: {
                let mut non_voters = self.non_voters.clone();
                non_voters.insert(id, Arc::new(info));
                non_voters
            },
        })
    }

    pub(crate) fn remove_node(&self, id: NodeId) -> Result<Self> {
        if !self.is_member(id) && !self.is_non_voter(id) {
            return Err(RaftError::UnknownNode(id));
        }
        let mut new_members = self.members.clone();
        let mut new_non_voters = self.non_voters.clone();
        new_members.remove(&id);
        new_non_voters.remove(&id);
        Ok(Self {
            members: new_members,
            non_voters: new_non_voters,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[non_exhaustive]
pub enum EntryDetail<N, D> {
    Normal(D),
    Blank,
    ChangeMemberShip(MemberShipConfig<N>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[non_exhaustive]
pub struct Entry<N, D> {
    pub term: TermId,
    pub index: LogIndex,
    pub detail: EntryDetail<N, D>,
}

#[derive(Serialize, Deserialize)]
#[non_exhaustive]
pub struct Metrics<N> {
    pub id: NodeId,
    pub role: Role,
    pub current_term: TermId,
    pub last_log_index: LogIndex,
    pub last_applied: LogIndex,
    pub leader: Option<NodeId>,
    pub membership: MemberShipConfig<N>,
}

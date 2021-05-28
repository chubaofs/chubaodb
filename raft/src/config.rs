use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,
    #[serde(default = "default_election_timeout_min")]
    pub election_timeout_min: u64,
    #[serde(default = "default_election_timeout_max")]
    pub election_timeout_max: u64,
    #[serde(default = "default_max_payload_entries")]
    pub max_payload_entries: usize,
    #[serde(default = "default_to_voter_threshold")]
    pub to_voter_threshold: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat_interval: default_heartbeat_interval(),
            election_timeout_min: default_election_timeout_min(),
            election_timeout_max: default_election_timeout_max(),
            max_payload_entries: default_max_payload_entries(),
            to_voter_threshold: default_to_voter_threshold(),
        }
    }
}

fn default_heartbeat_interval() -> u64 {
    150
}

fn default_election_timeout_min() -> u64 {
    300
}

fn default_election_timeout_max() -> u64 {
    600
}

fn default_max_payload_entries() -> usize {
    1000
}

fn default_to_voter_threshold() -> usize {
    100
}

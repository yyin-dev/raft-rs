use std::{
    fmt::Display,
    ops::{Index, RangeFrom},
};

use crate::proto::raftpb::Entry;

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

pub fn entries_to_str(entries: &Vec<Entry>) -> String {
    let v: Vec<_> = entries.iter().map(|e| e.to_string()).collect();
    format!("len={}, {}", entries.len(), v.join(", "))
}

#[derive(Message, Clone)]
pub struct Log {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<Entry>,
    #[prost(uint64, tag = "2")]
    pub last_included_index: u64,
    #[prost(uint64, tag = "3")]
    pub last_included_term: u64,
}

impl Log {
    pub fn new() -> Self {
        Self {
            entries: vec![],
            last_included_index: 0,
            last_included_term: 0,
        }
    }

    pub fn physical_len(&self) -> usize {
        self.entries.len()
    }

    // returns the logical length of the log
    pub fn len(&self) -> usize {
        let included_len = self.last_included_index as usize + 1;
        included_len + self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        return self.len() == 0;
    }

    pub fn last_term(&self) -> u64 {
        if self.entries.is_empty() {
            self.last_included_term
        } else {
            self.entries.last().unwrap().term
        }
    }

    pub fn append(&mut self, xs: &mut Vec<Entry>) {
        self.entries.append(xs)
    }

    // idx: logical index
    pub fn truncate(&mut self, idx: usize) {
        self.entries
            .truncate(idx - self.last_included_index as usize - 1)
    }

    pub fn push(&mut self, e: Entry) {
        self.entries.push(e)
    }

    pub fn term_at(&self, idx: usize) -> u64 {
        if idx == self.last_included_index as usize {
            self.last_included_term
        } else {
            self[idx].term
        }
    }
}

impl Index<usize> for Log {
    type Output = Entry;

    // idx: logical index
    fn index(&self, idx: usize) -> &Self::Output {
        if idx <= self.last_included_index as usize {
            panic!(
                "idx={}, last_included_index={}",
                idx, self.last_included_index
            )
        }

        &self.entries[idx - self.last_included_index as usize - 1]
    }
}

impl Index<RangeFrom<usize>> for Log {
    type Output = [Entry];

    fn index(&self, range_from: RangeFrom<usize>) -> &Self::Output {
        let start = range_from.start;
        if start <= self.last_included_index as usize {
            panic!(
                "start={}, last_included_index={}",
                start, self.last_included_index
            )
        }
        &self.entries[(start - self.last_included_index as usize - 1)..]
    }
}

impl Display for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "last_included_index={}, last_included_term={}, {}",
            self.last_included_index,
            self.last_included_term,
            entries_to_str(&self.entries)
        )
    }
}

#[derive(Message)]
pub struct PersistentState {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, optional, tag = "2")]
    pub voted_for: Option<u64>,
    #[prost(message, repeated, tag = "3")]
    pub entries: Vec<Entry>,
    #[prost(uint64, tag = "4")]
    pub last_included_index: u64,
    #[prost(uint64, tag = "5")]
    pub last_included_term: u64,
}

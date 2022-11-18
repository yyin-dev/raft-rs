use futures::executor::block_on;
use nanoid::nanoid;
use std::fmt;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use crate::proto::kvraftpb::*;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    curr_leader: AtomicUsize,
    cid: String,
    next_seq_num: AtomicU64,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        Clerk {
            name,
            servers,
            curr_leader: AtomicUsize::new(0),
            cid: nanoid!(),
            next_seq_num: AtomicU64::new(0),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub async fn get_async(&self, key: String) -> String {
        let args = GetRequest {
            key,
            cid: self.cid.clone(),
            seq_num: self.next_seq_num(),
        };

        let mut s = self.curr_leader.load(SeqCst);
        loop {
            let res = self.servers[s].get(&args).await;
            if let Ok(reply) = res {
                if !reply.wrong_leader {
                    self.curr_leader.store(s, SeqCst);
                    return reply.value;
                }
            }

            s = (s + 1) % self.servers.len();
        }
    }

    pub fn get(&self, key: String) -> String {
        block_on(self.get_async(key))
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    async fn put_append(&self, op: Op) {
        let args = match op {
            Op::Put(key, value) => PutAppendRequest {
                key,
                value,
                op: 1,
                cid: self.cid.clone(),
                seq_num: self.next_seq_num(),
            },
            Op::Append(key, value) => PutAppendRequest {
                key,
                value,
                op: 2,
                cid: self.cid.clone(),
                seq_num: self.next_seq_num(),
            },
        };

        let mut s = self.curr_leader.load(SeqCst);
        loop {
            let res = self.servers[s].put_append(&args).await;
            let ok = if let Ok(reply) = res {
                !reply.wrong_leader && reply.err.is_empty()
            } else {
                false
            };

            if ok {
                self.curr_leader.store(s, SeqCst);
                break;
            }

            s = (s + 1) % self.servers.len();
        }
    }

    pub async fn put_async(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value)).await
    }

    pub fn put(&self, key: String, value: String) {
        block_on(self.put_append(Op::Put(key, value)))
    }

    pub async fn append_async(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value)).await
    }

    pub fn append(&self, key: String, value: String) {
        block_on(self.put_append(Op::Append(key, value)))
    }

    fn next_seq_num(&self) -> u64 {
        self.next_seq_num.fetch_add(1, SeqCst)
    }
}

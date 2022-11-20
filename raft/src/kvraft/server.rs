use futures::channel::mpsc::{channel, unbounded, Sender, UnboundedReceiver, UnboundedSender};
use futures::executor::ThreadPool;
use futures::task::SpawnExt;
use futures::{select, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::proto::kvraftpb::*;
use crate::raft::{self, ApplyMsg};

struct AppliedCmd {
    cid: String,
    seq_num: u64,
    value: Option<String>,
}

#[derive(Debug)]
enum ServerError {
    WrongLeader,
}

type OpResultTx = Sender<Result<AppliedCmd, ServerError>>;

enum Event {
    Get((GetRequest, OpResultTx)),
    PutAppend((PutAppendRequest, OpResultTx)),
}

impl std::fmt::Display for GetRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Get({}, {}, {})", self.key, self.cid, self.seq_num)
    }
}

impl std::fmt::Display for PutAppendRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.op {
            0 => write!(f, "Unknown"),
            1 => write!(
                f,
                "Put({}, '{}', {}, {})",
                self.key, self.value, self.cid, self.seq_num
            ),
            2 => {
                write!(
                    f,
                    "Append({}, '{}', {}, {})",
                    self.key, self.value, self.cid, self.seq_num
                )
            }
            _ => unimplemented!(),
        }
    }
}

pub struct KvServer {
    pub rf: raft::Node,
    pub me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,

    data: HashMap<String, String>,
    latest_seq_nums: HashMap<String, u64>,
    apply_ch: Option<UnboundedReceiver<ApplyMsg>>,
    started_cmds: HashMap<u64, OpResultTx>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        let server = KvServer {
            rf: raft::Node::new(rf),
            me,
            maxraftstate,
            data: HashMap::new(),
            latest_seq_nums: HashMap::new(),
            apply_ch: Some(apply_ch),
            started_cmds: HashMap::new(),
        };

        server
    }

    fn is_duplicate(&mut self, cid: &str, seq_num: u64) -> bool {
        if !self.latest_seq_nums.contains_key(cid) {
            self.latest_seq_nums.insert(cid.to_string(), 0);
        }

        *self.latest_seq_nums.get(cid).unwrap() >= seq_num
    }

    fn record_seen_seq_num(&mut self, cid: &str, seq_num: u64) {
        if !self.latest_seq_nums.contains_key(cid) {
            self.latest_seq_nums.insert(cid.to_string(), 0);
        }

        assert!(*self.latest_seq_nums.get(cid).unwrap() <= seq_num);

        self.latest_seq_nums.insert(cid.to_string(), seq_num);
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::Get((arg, mut tx)) => match self.rf.start(&arg) {
                Ok((idx, _)) => {
                    info!("[{}] starts {}", self.me, arg);
                    self.started_cmds.insert(idx, tx);
                }
                Err(_) => {
                    tx.try_send(Err(ServerError::WrongLeader)).unwrap();
                }
            },
            Event::PutAppend((arg, mut tx)) => match self.rf.start(&arg) {
                Ok((idx, _)) => {
                    info!("[{}] starts {}", self.me, arg);
                    self.started_cmds.insert(idx, tx);
                }
                Err(_) => {
                    tx.try_send(Err(ServerError::WrongLeader)).unwrap();
                }
            },
        };
    }

    fn apply(&mut self, apply_msg: raft::ApplyMsg) {
        match apply_msg {
            ApplyMsg::Command { data, index } => {
                let (cid, seq_num, key, is_get) =
                    if let Ok(req) = labcodec::decode::<GetRequest>(&data) {
                        (req.cid, req.seq_num, req.key, true)
                    } else if let Ok(req) = labcodec::decode::<PutAppendRequest>(&data) {
                        (req.cid, req.seq_num, req.key, false)
                    } else {
                        panic!("decode error");
                    };

                if self.is_duplicate(&cid, seq_num) {
                    let applied_cmd = AppliedCmd {
                        cid,
                        seq_num,
                        value: if is_get {
                            Some(self.data.get(&key).cloned().unwrap_or(String::new()))
                        } else {
                            None
                        },
                    };
                    self.started_cmds
                        .remove(&index)
                        .map(|mut tx| tx.try_send(Ok(applied_cmd)));
                    return;
                }

                let applied_cmd = if let Ok(get_request) = labcodec::decode::<GetRequest>(&data) {
                    info!("[{}] applies {}", self.me, get_request);
                    let value = match self.data.get(&get_request.key) {
                        None => String::new(),
                        Some(v) => v.clone(),
                    };

                    AppliedCmd {
                        cid: get_request.cid,
                        seq_num: get_request.seq_num,
                        value: Some(value),
                    }
                } else if let Ok(put_append_request) = labcodec::decode::<PutAppendRequest>(&data) {
                    info!("[{}] applies {}", self.me, put_append_request);
                    match put_append_request.op {
                        0 => panic!("unknown op?"),
                        1 => {
                            /* put */
                            self.data.insert(
                                put_append_request.key.clone(),
                                put_append_request.value.clone(),
                            );
                        }
                        2 => {
                            /* append */
                            self.data
                                .get_mut(&put_append_request.key)
                                .map(|s| s.push_str(&put_append_request.value));
                        }
                        op => panic!("unexpected op: {}", op),
                    };

                    AppliedCmd {
                        cid: put_append_request.cid,
                        seq_num: put_append_request.seq_num,
                        value: None,
                    }
                } else {
                    panic!("decode error");
                };

                self.record_seen_seq_num(&cid, seq_num);

                self.started_cmds
                    .remove(&index)
                    .map(|mut tx| tx.try_send(Ok(applied_cmd)));
            }
            ApplyMsg::Snapshot {
                data: _,
                term: _,
                index: _,
            } => todo!(),
        }
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    me: usize,
    server: Arc<Mutex<KvServer>>,
    event_tx: UnboundedSender<Event>,
}

impl Node {
    pub fn new(mut kv: KvServer) -> Node {
        let (event_tx, event_rx) = unbounded();
        let executor = kv.rf.executor.clone();
        let apply_ch = kv.apply_ch.take().unwrap();

        let mut node = Node {
            me: kv.me,
            server: Arc::new(Mutex::new(kv)),
            event_tx,
        };

        node.event_loop(event_rx, apply_ch, executor);

        node
    }

    fn event_loop(
        &mut self,
        mut event_rx: UnboundedReceiver<Event>,
        mut apply_ch: UnboundedReceiver<ApplyMsg>,
        executor: ThreadPool,
    ) {
        let server = self.server.clone();

        executor
            .spawn(async move {
                loop {
                    select! {
                        event = event_rx.select_next_some() => {
                            server.lock().unwrap().handle_event(event);
                        }
                        apply_msg = apply_ch.select_next_some() => {
                            server.lock().unwrap().apply(apply_msg);
                        }
                    }
                }
            })
            .unwrap()
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        self.server.lock().unwrap().rf.get_state()
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        info!("[{}] receives {}", self.me, arg);

        let (tx, mut rx) = channel(1);
        self.event_tx
            .unbounded_send(Event::Get((arg.clone(), tx)))
            .unwrap();
        let res = match rx.next().await.unwrap() {
            Ok(applied_cmd) => {
                if applied_cmd.cid == arg.cid && applied_cmd.seq_num == arg.seq_num {
                    Ok(GetReply {
                        wrong_leader: false,
                        err: String::new(),
                        value: applied_cmd.value.unwrap(),
                    })
                } else {
                    Ok(GetReply {
                        wrong_leader: false,
                        err: String::from("not committed"),
                        value: String::new(),
                    })
                }
            }
            Err(ServerError::WrongLeader) => Ok(GetReply {
                wrong_leader: true,
                err: String::new(),
                value: String::new(),
            }),
        };

        info!("[{}] {} -> {:?}", self.me, arg, res);
        res
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        info!("[{}] receives {}", self.me, arg);

        let (tx, mut rx) = channel(1);
        self.event_tx
            .unbounded_send(Event::PutAppend((arg.clone(), tx)))
            .unwrap();

        let res = match rx.next().await.unwrap() {
            Ok(applied_cmd) => {
                if applied_cmd.cid == arg.cid && applied_cmd.seq_num == arg.seq_num {
                    Ok(PutAppendReply {
                        wrong_leader: false,
                        err: String::new(),
                    })
                } else {
                    Ok(PutAppendReply {
                        wrong_leader: false,
                        err: String::from("not committed"),
                    })
                }
            }
            Err(ServerError::WrongLeader) => Ok(PutAppendReply {
                wrong_leader: true,
                err: String::new(),
            }),
        };

        info!("[{}] {} -> {:?}", self.me, arg, res);

        res
    }
}

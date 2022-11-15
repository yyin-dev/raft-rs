use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::executor::ThreadPool;
use futures::task::SpawnExt;
use futures::{select, FutureExt, StreamExt};
use log::info;
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

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

struct LogEntry {
    term: u64,
    // cmd: Vec<u8>,
}

#[derive(PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

enum Event {
    ElectionTimeout,
    HeartbeatTimeout,
    RequestVoteReply(RequestVoteReply),
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // role
    role: Role,
    // current term
    curr_term: u64,
    // voted for in current term
    voted_for: Option<usize>,
    // log entries
    log: Vec<LogEntry>,

    // States for event handling
    votes_received: u64,

    // event sender
    event_tx: Option<UnboundedSender<Event>>,
    // apply channel
    apply_ch: UnboundedSender<ApplyMsg>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            role: Role::Follower,
            curr_term: 0,
            voted_for: None,
            log: vec![],
            votes_received: 0,
            event_tx: None,
            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf.as_follower(rf.curr_term);

        info!("[{}] started", rf.me);
        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    fn as_follower(&mut self, new_term: u64) {
        info!("[{}] -> follower, term={}", self.me, new_term);
        self.curr_term = new_term;
        self.role = Role::Follower;
    }

    fn as_candidate(&mut self) {
        self.curr_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.me);
        self.votes_received = 0;
        info!("[{}] -> candidate, term={}", self.me, self.curr_term);
    }

    fn as_leader(&mut self) {
        info!("[{}] -> leader, term={}", self.me, self.curr_term);
        self.role = Role::Leader;
    }

    fn request_vote_arg(&self) -> RequestVoteArgs {
        let last_log_idx = self.log.len() as i64 - 1;
        let last_log_term = if last_log_idx >= 0 {
            self.log.get(last_log_idx as usize).unwrap().term
        } else {
            0
        };

        RequestVoteArgs {
            term: self.curr_term,
            candidate_id: self.me as u64,
            last_log_idx,
            last_log_term,
        }
    }

    fn other_peers(&self) -> impl Iterator<Item = usize> + '_ {
        let range = 0..self.peers.len();
        range.into_iter().filter(move |p| *p != self.me)
    }

    fn majority(&self) -> u64 {
        self.peers.len() as u64 / 2 + 1
    }

    fn run_election(&mut self) {
        // becomes candidate: increment current term, vote for self
        self.as_candidate();

        // send RequestVote to others in parallel
        for p in self.other_peers() {
            info!("[{}] send RequestVote to [{}]", self.me, p);
            self.send_request_vote(p, self.request_vote_arg());
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(&self, server: usize, args: RequestVoteArgs) {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```

        let peer = &self.peers[server];
        let peer_clone = peer.clone();

        let event_tx = self.event_tx.as_ref().unwrap().clone();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            if let Ok(reply) = res {
                event_tx
                    .unbounded_send(Event::RequestVoteReply(reply))
                    .unwrap();
            }
        });
    }

    fn handle_term(&mut self, term: u64) {
        if term > self.curr_term {
            self.as_follower(term);
        }
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        info!(
            "[{}] RequestVote from [{}], term=[{}]",
            self.me, args.candidate_id, args.term
        );

        self.handle_term(args.term);

        let vote_granted = if args.term < self.curr_term {
            false
        } else {
            let never_voted = self.voted_for.is_none();
            let same_vote = self
                .voted_for
                .map_or(false, |v| v as u64 == args.candidate_id);

            never_voted || same_vote
        };

        if vote_granted {
            self.voted_for = Some(args.candidate_id as usize);
        }

        RequestVoteReply {
            term: self.curr_term,
            vote_granted,
        }
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::ElectionTimeout => {
                if self.role != Role::Leader {
                    self.run_election();
                }
            }
            Event::HeartbeatTimeout => {}
            Event::RequestVoteReply(reply) => {
                info!(
                    "[{}] get RequestVote reply, term={}, granted={}",
                    self.me, reply.term, reply.vote_granted
                );
                if reply.term > self.curr_term {
                    self.as_follower(reply.term);
                } else if reply.term == self.curr_term {
                    self.votes_received += 1;
                    if self.votes_received >= self.majority() {
                        self.as_leader();
                        // TODO: Send AppendEntries heartbeat immediately
                    }
                } else {
                    unreachable!()
                }
            }
        }
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        self.persist();
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
        let _ = &self.apply_ch;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    executor: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        let (event_tx, event_rx) = unbounded();

        raft.event_tx = Some(event_tx.clone());

        let mut node = Node {
            raft: Arc::new(Mutex::new(raft)),
            executor: ThreadPool::new().unwrap(),
        };

        node.start_event_loop(event_tx, event_rx);

        node
    }

    fn start_event_loop(
        &mut self,
        event_tx: UnboundedSender<Event>,
        mut event_rx: UnboundedReceiver<Event>,
    ) {
        let raft = self.raft.clone();

        let reset_elec_timeout = || {
            futures_timer::Delay::new(Duration::from_millis(
                rand::thread_rng().gen_range(100, 500),
            ))
            .fuse() // select! requires FuseFuture
        };
        let mut elec_timeout = reset_elec_timeout();

        let reset_heartbeat_timeout =
            || futures_timer::Delay::new(Duration::from_millis(100)).fuse(); // select! requires FuseFuture
        let mut heartbeat_timeout = reset_heartbeat_timeout();

        self.executor
            .spawn(async move {
                loop {
                    select! {
                        e = event_rx.select_next_some() => {
                            raft.lock().unwrap().handle_event(e);
                        },
                        _ = elec_timeout => {
                            event_tx.unbounded_send(Event::ElectionTimeout).unwrap();
                            elec_timeout = reset_elec_timeout();
                        },
                        _ = heartbeat_timeout =>  {
                            event_tx.unbounded_send(Event::HeartbeatTimeout).unwrap();
                            heartbeat_timeout = reset_heartbeat_timeout();
                        }
                    }
                }
            })
            .unwrap();
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().curr_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().role == Role::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        Ok(self.raft.lock().unwrap().handle_request_vote(args))
    }
}

use crate::raft::states::Log;
pub use crate::raft::states::{entries_to_str, PersistentState, State};
use futures::channel::mpsc::{channel, unbounded, Sender, UnboundedReceiver, UnboundedSender};
use futures::executor::ThreadPool;
use futures::task::SpawnExt;
use futures::{select, FutureExt, StreamExt};
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::time::Duration;

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let m: config::Entry = labcodec::decode(&self.cmd).unwrap();
        write!(f, "[term={}, cmd={}]", self.term, m.x)
    }
}

// Note: #cfg[(test)] only applies to one line below.
// #[cfg(test)] // To print commands in a readable way for debugging
pub mod config;

pub mod errors;
pub mod persister;
pub mod states;

#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

/// Macro for logging message combined with state of the Raft peer.
macro_rules! rlog {
    (level: $level:ident, $raft:expr, $($arg:tt)+) => {
        ::log::$level!("[#{} @{} as {:?}] {}", $raft.me, $raft.curr_term, $raft.role, format_args!($($arg)+))
    };
    ($raft:expr, $($arg:tt)+) => {
        rlog!(level: info, $raft, $($arg)+)
    };
}

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

#[derive(PartialEq, Eq, Debug)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

enum Event {
    ResetElectionTimeout,
    ElectionTimeout,
    HeartbeatTimeout,
    RequestVoteArgs((RequestVoteArgs, Sender<RequestVoteReply>)),
    RequestVoteReplyMsg((usize, RequestVoteReply, RequestVoteArgs)),
    AppendEntriesArgs((AppendEntriesArgs, Sender<AppendEntriesReply>)),
    AppendEntriesReplyMsg((usize, AppendEntriesReply, AppendEntriesArgs)),
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
    // log
    log: Log,
    // commit index
    commit_idx: u64,
    // last applied entry index
    last_applied: u64,
    // index of next log entry to send for log replication (this is a guess)
    next_idx: Vec<u64>,
    // index of the highest log entry known to be replicated (this is a truth)
    match_idx: Vec<u64>,

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

        let n = peers.len();
        let mut rf = Raft {
            peers,
            persister,
            me,
            role: Role::Follower,
            curr_term: 0,
            voted_for: None,
            log: Log::new(),
            commit_idx: 0,
            last_applied: 0,
            next_idx: vec![1; n],
            match_idx: vec![0; n],
            votes_received: 0,
            event_tx: None,
            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf.as_follower(rf.curr_term);

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

        let s = PersistentState {
            term: self.curr_term,
            voted_for: self.voted_for.map(|p| p as u64),
            entries: self.log.entries.clone(),
            last_included_index: self.log.last_included_index,
            last_included_term: self.log.last_included_term,
        };

        let mut buf: Vec<u8> = vec![];
        labcodec::encode(&s, &mut buf).unwrap();
        self.persister.save_raft_state(buf);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
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
        match labcodec::decode::<PersistentState>(data) {
            Ok(s) => {
                self.curr_term = s.term;
                self.voted_for = s.voted_for.map(|p| p as usize);
                self.log = Log {
                    entries: s.entries,
                    last_included_index: s.last_included_index,
                    last_included_term: s.last_included_term,
                }
            }
            Err(e) => {
                panic!("decode error: {:?}", e);
            }
        }
    }

    fn as_follower(&mut self, new_term: u64) {
        rlog!(self, "-> follower, term={}", new_term);
        self.curr_term = new_term;
        self.role = Role::Follower;
        self.voted_for = None;
    }

    fn as_candidate(&mut self) {
        self.curr_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.me);
        self.votes_received = 1;
        rlog!(self, "-> candidate, term={}", self.curr_term);
    }

    fn as_leader(&mut self) {
        rlog!(
            self,
            "-> leader, term={}, log: {}",
            self.curr_term,
            self.log
        );
        self.role = Role::Leader;
        self.next_idx = vec![self.log.len() as u64; self.next_idx.len()];
        self.match_idx = vec![0; self.match_idx.len()];
    }

    fn other_peers(&self) -> impl Iterator<Item = usize> + '_ {
        let range = 0..self.peers.len();
        range.into_iter().filter(move |p| *p != self.me)
    }

    fn majority(&self) -> u64 {
        self.peers.len() as u64 / 2 + 1
    }

    fn request_vote_arg(&self) -> RequestVoteArgs {
        let last_log_idx = self.log.len() as u64 - 1;
        let last_log_term = self.log.term_at(last_log_idx as usize);

        RequestVoteArgs {
            term: self.curr_term,
            candidate_id: self.me as u64,
            last_log_idx,
            last_log_term,
        }
    }

    fn append_entries_arg(&self, peer: usize) -> AppendEntriesArgs {
        let prev_log_idx = self.next_idx[peer] - 1;
        AppendEntriesArgs {
            term: self.curr_term,
            leader_id: self.me as u64,
            prev_log_idx,
            prev_log_term: self.log.term_at(prev_log_idx as usize),
            entries: self.log[self.next_idx[peer] as usize..].to_vec(),
            leader_commit: self.commit_idx,
        }
    }

    fn run_election(&mut self) {
        // becomes candidate: increment current term, vote for self
        self.as_candidate();
        self.persist();

        // send RequestVote to others in parallel
        for p in self.other_peers() {
            rlog!(self, "send RequestVote to [{}]", p);
            self.send_request_vote(p, self.request_vote_arg());
        }
    }

    fn send_heartbeats(&mut self) {
        for p in self.other_peers() {
            self.send_append_entries(p, self.append_entries_arg(p));
        }
    }

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
        let peer = &self.peers[server];
        let peer_clone = peer.clone();

        let event_tx = self.event_tx.as_ref().unwrap().clone();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            if let Ok(reply) = res {
                event_tx
                    .unbounded_send(Event::RequestVoteReplyMsg((server, reply, args)))
                    .unwrap();
            }
        });
    }

    fn send_append_entries(&self, server: usize, args: AppendEntriesArgs) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();

        let event_tx = self.event_tx.as_ref().unwrap().clone();
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            if let Ok(reply) = res {
                event_tx
                    .unbounded_send(Event::AppendEntriesReplyMsg((server, reply, args)))
                    .unwrap();
            }
        });
    }

    fn apply_committed(&mut self) {
        for i in self.last_applied + 1..self.commit_idx + 1 {
            self.apply_ch
                .unbounded_send(ApplyMsg::Command {
                    data: self.log[i as usize].cmd.clone(),
                    index: i,
                })
                .unwrap();
        }
        self.last_applied = self.commit_idx;
    }

    // Step down to follower if term > self.term
    fn handle_term(&mut self, term: u64) -> bool {
        if term > self.curr_term {
            self.as_follower(term);
            self.persist();
            true
        } else {
            false
        }
    }

    fn handle_request_vote_request(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        self.handle_term(args.term);

        if args.term < self.curr_term {
            return RequestVoteReply {
                term: self.curr_term,
                vote_granted: false,
            };
        }

        let can_vote =
            self.voted_for.is_none() || self.voted_for.unwrap() as u64 == args.candidate_id;

        let log_up_to_date = args.last_log_term > self.log.last_term()
            || args.last_log_term == self.log.last_term()
                && args.last_log_idx >= self.log.len() as u64 - 1;

        let vote_granted = can_vote && log_up_to_date;

        if vote_granted {
            self.voted_for = Some(args.candidate_id as usize);
            self.persist();
        }

        rlog!(
            self,
            "RequestVote from [{}], term={}, last_log_term={}, last_log_idx={}, granted={}",
            args.candidate_id,
            args.term,
            args.last_log_term,
            args.last_log_idx,
            vote_granted
        );

        RequestVoteReply {
            term: self.curr_term,
            vote_granted,
        }
    }

    fn handle_request_vote_reply(
        &mut self,
        from: usize,
        reply: RequestVoteReply,
        args: RequestVoteArgs,
    ) {
        rlog!(
            self,
            "get RequestVote reply from [{}], term={}, granted={}",
            from,
            reply.term,
            reply.vote_granted
        );

        self.handle_term(reply.term);
        if self.role != Role::Candidate {
            return;
        }

        // term confusion!
        if reply.term != args.term || args.term != self.curr_term {
            return;
        }

        if reply.term == self.curr_term && reply.vote_granted {
            self.votes_received += 1;
            if self.votes_received >= self.majority() {
                self.as_leader();
                self.persist();

                self.send_heartbeats();
            }
        }
    }

    fn handle_append_entries_request(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        rlog!(self, "received AppendEntries from [{}], term={}, prev_log_idx: {}, prev_log_term: {}, entries: {}, curr_log: {}", 
            args.leader_id, args.term, args.prev_log_idx, args.prev_log_term, entries_to_str(&args.entries), self.log);
        self.handle_term(args.term);

        let mut false_reply = AppendEntriesReply {
            term: self.curr_term,
            success: false,
            conflict_idx: 0,
        };

        if args.term < self.curr_term {
            return false_reply;
        }

        // Valid AppendEntries from current leader, reset election timeout
        self.event_tx
            .as_ref()
            .unwrap()
            .unbounded_send(Event::ResetElectionTimeout)
            .unwrap();

        // (Simplified) fast rollback using only conflict_index
        // Described in https://thesquareplanet.com/blog/students-guide-to-raft/

        // Reply false if the log doesn't match at prevLogIndex
        if self.log.len() <= args.prev_log_idx as usize {
            false_reply.conflict_idx = self.log.len() as u64;
            return false_reply;
        }

        if self.log.term_at(args.prev_log_idx as usize) != args.prev_log_term {
            let conflict_term = self.log.term_at(args.prev_log_idx as usize);
            let mut conflict_idx = args.prev_log_idx as usize;
            while conflict_idx > 0 && self.log.term_at(conflict_idx - 1) == conflict_term {
                conflict_idx -= 1;
            }

            false_reply.conflict_idx = conflict_idx as u64;
            return false_reply;
        }

        // Find conflict
        let check_end = std::cmp::min(
            self.log.len(),
            args.prev_log_idx as usize + 1 + args.entries.len(),
        );
        let mut conflict_idx = check_end;
        for i in args.prev_log_idx as usize + 1..check_end {
            let j = i - 1 - args.prev_log_idx as usize;
            if self.log.term_at(i) != args.entries[j].term {
                conflict_idx = i;
                break;
            }
        }

        // Delete conflict entry and all after it
        self.log.truncate(conflict_idx);

        // Append any new entries
        self.log
            .append(&mut args.entries[(conflict_idx - 1 - args.prev_log_idx as usize)..].to_vec());

        // if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if args.leader_commit > self.commit_idx {
            rlog!(
                self,
                "commit_idx {} < leader_commit {}",
                self.commit_idx,
                args.leader_commit
            );
            self.commit_idx = std::cmp::min(
                args.leader_commit,
                args.prev_log_idx + args.entries.len() as u64,
            );

            // apply
            self.apply_committed();
        }

        self.persist();
        AppendEntriesReply {
            term: self.curr_term,
            success: true,
            conflict_idx: conflict_idx as u64,
        }
    }

    fn handle_append_entries_reply(
        &mut self,
        from: usize,
        reply: AppendEntriesReply,
        args: AppendEntriesArgs,
    ) {
        rlog!(
            self,
            "term={} get AppendEntries reply from [{}], term={}, success={}",
            self.curr_term,
            from,
            reply.term,
            reply.success
        );

        self.handle_term(reply.term);
        if self.role != Role::Leader {
            rlog!(self, "AppendEntries to [{}] failed: term passed", from);
            return;
        }

        // term confusion!
        if reply.term != args.term || args.term != self.curr_term {
            return;
        }

        if reply.success {
            self.next_idx[from] = args.prev_log_idx + args.entries.len() as u64 + 1;
            self.match_idx[from] = args.prev_log_idx + args.entries.len() as u64;
        } else {
            // There are two reasons for failing: term passed, log inconsitency.
            // We would have already returned if term passed, so must be due to log inconsistency.

            rlog!(
                self,
                "AppendEntries to [{}] failed: log inconsistency",
                from,
            );

            // Fast rollback
            // Simply setting self.next_idx[from] is wrong.
            // Reason: in unreliable network, message can be repeated. So you can get repeated
            // "log inconsistency" replies for one AppendEntries RPC. This can cause you to
            // decrease next_idx more than you should (with normal rollback), or increase
            // next_idx by mistake (with fast rollback).
            self.next_idx[from] = std::cmp::min(self.next_idx[from], reply.conflict_idx);

            self.send_append_entries(from, self.append_entries_arg(from));
        }

        // Advance commit_idx
        for n in self.commit_idx + 1..self.log.len() as u64 {
            let replicated_on = 1 + self
                .other_peers()
                .filter(|p| self.match_idx[*p] >= n)
                .count();
            if replicated_on as u64 >= self.majority()
                && self.log.term_at(n as usize) == self.curr_term
            {
                self.commit_idx = n;
            }
        }
        self.persist();
        self.apply_committed();
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::ResetElectionTimeout => unreachable!(),
            Event::ElectionTimeout => {
                if self.role != Role::Leader {
                    self.run_election();
                }
            }
            Event::HeartbeatTimeout => {
                if self.role == Role::Leader {
                    self.send_heartbeats();
                }
            }
            Event::RequestVoteArgs((args, mut tx)) => {
                tx.try_send(self.handle_request_vote_request(args)).unwrap()
            }
            Event::RequestVoteReplyMsg((from, reply, args)) => {
                self.handle_request_vote_reply(from, reply, args)
            }
            Event::AppendEntriesArgs((args, mut tx)) => {
                tx.try_send(self.handle_append_entries_request(args))
                    .unwrap();
            }
            Event::AppendEntriesReplyMsg((from, reply, args)) => {
                self.handle_append_entries_reply(from, reply, args)
            }
        }
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.role == Role::Leader {
            let index = self.log.len() as u64;
            let term = self.curr_term;

            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;

            self.log.push(Entry {
                term,
                cmd: buf.clone(),
            });

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
        rlog!(self, "Raft::cond_install_snapshot");
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, _snapshot: &[u8]) {
        // Your code here (2D).
        rlog!(self, "creating snapshot, index={}", index);
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
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
    event_tx: UnboundedSender<Event>,
    pub executor: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        let (event_tx, event_rx) = unbounded();

        raft.event_tx = Some(event_tx.clone());

        let mut node = Node {
            raft: Arc::new(Mutex::new(raft)),
            event_tx,
            executor: ThreadPool::new().unwrap(),
        };

        node.start_event_loop(event_rx);

        node
    }

    fn start_event_loop(&mut self, mut event_rx: UnboundedReceiver<Event>) {
        let raft = self.raft.clone();

        let reset_elec_timeout = || {
            futures_timer::Delay::new(Duration::from_millis(
                rand::thread_rng().gen_range(300, 500),
            ))
            .fuse() // select! requires FuseFuture
        };
        let mut elec_timeout = reset_elec_timeout();

        let reset_heartbeat_timeout =
            || futures_timer::Delay::new(Duration::from_millis(100)).fuse(); // select! requires FuseFuture
        let mut heartbeat_timeout = reset_heartbeat_timeout();

        let event_tx = self.event_tx.clone();
        self.executor
            .spawn(async move {
                loop {
                    select! {
                        e = event_rx.select_next_some() => {
                            if let Event::ResetElectionTimeout = e {
                                elec_timeout = reset_elec_timeout();
                            } else {
                                raft.lock().unwrap().handle_event(e);
                            }
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
        self.raft.lock().unwrap().start(command)
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

        self.raft.lock().unwrap().cond_install_snapshot(
            last_included_term,
            last_included_index,
            snapshot,
        )
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)

        self.raft.lock().unwrap().snapshot(index, snapshot)
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // With locking
        // Ok(self.raft.lock().unwrap().handle_request_vote_request(args))

        // Without locking
        let (tx, mut rx) = channel(1);
        self.event_tx
            .unbounded_send(Event::RequestVoteArgs((args, tx)))
            .unwrap();

        Ok(rx.next().await.unwrap())
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        // With locking
        // Ok(self.raft.lock().unwrap().handle_append_entries_request(args))

        // Without locking
        let (tx, mut rx) = channel(1);
        self.event_tx
            .unbounded_send(Event::AppendEntriesArgs((args, tx)))
            .unwrap();

        Ok(rx.next().await.unwrap())
    }
}

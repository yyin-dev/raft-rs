use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use crate::{msg::*, service::*, u8_vec_to_str, *};

macro_rules! slog {
    (level: $level:ident, $($arg:tt)+) => {
        ::log::$level!($($arg)+)
    };
    ($($arg:tt)+) => {
        slog!(level: info, $($arg)+)
    };
}

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    next_ts: Arc<Mutex<u64>>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, req: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        let mut next_ts = self.next_ts.lock().unwrap();
        let resp = TimestampResponse { ts: *next_ts };
        *next_ts += 1;
        Ok(resp)
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Timestamp(u64),
    Vector((Vec<u8>, SystemTime)),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

#[derive(Clone)]
pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        let map = self.get_map(column);

        let ts_start = ts_start_inclusive.unwrap_or(0);
        let ts_end = ts_end_inclusive.unwrap_or(u64::MAX);

        use std::ops::Bound::Included;
        let start_key = Included((key.clone(), ts_start));
        let end_key = Included((key, ts_end));

        map.range((start_key, end_key)).next_back()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        let map = self.get_map_mut(column);
        map.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        let map = self.get_map_mut(column);
        map.remove(&(key, commit_ts));
    }

    // Find `data@start_ts` in the WRITE column for `key`
    fn find_data_written_at_ts(&self, key: Vec<u8>, start_ts: u64) -> Option<u64> {
        use std::ops::Bound::Included;
        let start_key = Included((key.clone(), 0));
        let end_key = Included((key, u64::MAX));
        for ((_, commit_ts), v) in self.write.range((start_key, end_key)) {
            match v {
                Value::Timestamp(ts) => {
                    if *ts == start_ts {
                        return Some(*commit_ts);
                    }
                }
                Value::Vector(_) => unreachable!(),
            }
        }

        None
    }

    fn get_map(&self, column: Column) -> &BTreeMap<Key, Value> {
        match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        }
    }

    fn get_map_mut(&mut self, column: Column) -> &mut BTreeMap<Key, Value> {
        match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        }
    }

    fn print(&self) {
        slog!("data: {:?}", self.data);
        slog!("lock: {:?}", self.lock);
        slog!("write: {:?}", self.write);
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        loop {
            let mut table = self.data.lock().unwrap();

            table.print();

            // Concurrent writes
            let conflict_write = table
                .read(req.key.clone(), Column::Lock, None, Some(req.start_ts))
                .map(|(key, ts)| (key.clone(), ts));

            if let Some(((_, conflict_ts), _)) = conflict_write {
                slog!(
                    "get(k={}): concurrent write, backoff",
                    u8_vec_to_str(&req.key)
                );

                std::mem::drop(table); // drop for backoff
                self.back_off_maybe_clean_up_lock(conflict_ts, req.key.clone());
                continue;
            }

            // Find latest write before start_ts
            let latest_write = table.read(req.key.clone(), Column::Write, None, Some(req.start_ts));
            match latest_write {
                None => return Ok(GetResponse { value: vec![] }),
                Some((_, v)) => match v {
                    Value::Vector(_) => unreachable!(),
                    Value::Timestamp(data_ts) => {
                        let (_, value) = table
                            .read(req.key, Column::Data, Some(*data_ts), Some(*data_ts))
                            .unwrap();

                        match value {
                            Value::Timestamp(_) => unreachable!(),
                            Value::Vector((value, _)) => {
                                return Ok(GetResponse {
                                    value: value.clone(),
                                })
                            }
                        }
                    }
                },
            }
        }
    }

    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        slog!(
            "prewrite(k={}, v={}, start_ts={}, primary={})",
            u8_vec_to_str(&req.key),
            u8_vec_to_str(&req.value),
            req.start_ts,
            u8_vec_to_str(&req.primary_key),
        );

        let mut table = self.data.lock().unwrap();

        table.print();

        // abort if there is write after our start_ts
        if table
            .read(req.key.clone(), Column::Write, Some(req.start_ts), None)
            .is_some()
        {
            slog!("failed: writes after me");
            return Ok(PrewriteResponse { ok: false });
        }

        // abort if there is locks at any timestamps
        if table
            .read(req.key.clone(), Column::Lock, None, None)
            .is_some()
        {
            slog!("failed: other locks");
            return Ok(PrewriteResponse { ok: false });
        }

        table.write(
            req.key.clone(),
            Column::Data,
            req.start_ts,
            Value::Vector((req.value.clone(), SystemTime::now())),
        );

        table.write(
            req.key.clone(),
            Column::Lock,
            req.start_ts,
            Value::Vector((req.primary_key, SystemTime::now())),
        );

        Ok(PrewriteResponse { ok: true })
    }

    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let mut table = self.data.lock().unwrap();

        if req.is_primary {
            // Check if aborted while working
            if table
                .read(
                    req.key.clone(),
                    Column::Lock,
                    Some(req.start_ts),
                    Some(req.start_ts),
                )
                .is_none()
            {
                return Ok(CommitResponse { ok: false });
            }
        }

        table.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );

        table.erase(req.key.clone(), Column::Lock, req.start_ts);

        Ok(CommitResponse { ok: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, conflict_start_ts: u64, key: Vec<u8>) {
        slog!(
            "back_off_maybe_clean_up_lock(conflict_start_ts={}, key={})",
            conflict_start_ts,
            u8_vec_to_str(&key)
        );

        // backoff
        thread::sleep(Duration::from_millis(100));

        // cleanup lock
        // Reference: https://tikv.org/deep-dive/distributed-transaction/percolator/#tolerating-crashes
        let mut table = self.data.lock().unwrap();

        let (_, primary) = table
            .read(key.clone(), Column::Lock, None, Some(conflict_start_ts))
            .unwrap();

        match primary {
            Value::Timestamp(_) => unreachable!(),
            Value::Vector((primary_key, _)) => {
                slog!("primary={}", u8_vec_to_str(primary_key));

                let primary_lock = table.read(
                    primary_key.clone(),
                    Column::Lock,
                    Some(conflict_start_ts),
                    Some(conflict_start_ts),
                );

                slog!("primary_lock: {:?}", primary_lock);

                if let Some((_, value)) = primary_lock {
                    // Primary lock still exists. Not committed yet.
                    // Kill if inactive for too long.
                    match value {
                        Value::Timestamp(_) => unreachable!(),
                        Value::Vector((v, lock_wallclock_time)) => {
                            let locked_for = SystemTime::now()
                                .duration_since(*lock_wallclock_time)
                                .unwrap();

                            if locked_for > Duration::from_nanos(TTL) {
                                // inactive, rollback by erase the current lock
                                table.erase(key, Column::Lock, conflict_start_ts);
                            }
                        }
                    }
                } else {
                    // Primary lock has been released
                    // Check primary key in the WRITE column.
                    // If there's a record 'data@conflict_start_ts' in the WRITE column,
                    // then the txn must have committed. Must roll-forward:
                    // commit write on this key and release the lock.
                    // Otherwise, the txn must have been rolledback. Release the lock.
                    if let Some(conflict_commit_ts) =
                        table.find_data_written_at_ts(primary_key.clone(), conflict_start_ts)
                    {
                        slog!("conflict_commit_ts: {}", conflict_commit_ts);

                        table.write(
                            key.clone(),
                            Column::Write,
                            conflict_commit_ts,
                            Value::Timestamp(conflict_start_ts),
                        );
                        table.erase(key, Column::Lock, conflict_start_ts);
                    } else {
                        table.erase(key, Column::Lock, conflict_start_ts);
                    }
                }
            }
        }
    }
}

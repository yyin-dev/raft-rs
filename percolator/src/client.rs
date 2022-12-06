use futures::executor::block_on;
use labrpc::*;
use std::{thread, time::Duration};

use crate::{
    msg::{CommitRequest, GetRequest, PrewriteRequest, TimestampRequest},
    service::{TSOClient, TransactionClient},
    u8_vec_to_str,
};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
// If you read the testcase, this include the initial request. So you make RETRY_TIME-1 retries.
const RETRY_TIMES: usize = 3;

macro_rules! clog {
    (level: $level:ident, $($arg:tt)+) => {
        ::log::$level!($($arg)+)
    };
    ($($arg:tt)+) => {
        clog!(level: info, $($arg)+)
    };
}

#[derive(Clone, Debug)]
pub struct Write {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
pub struct Txn {
    start_ts: u64,
    writes: Vec<Write>,
}

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    tso_service: TSOClient,
    txn_service: TransactionClient,
    txn: Option<Txn>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        Client {
            tso_service: tso_client,
            txn_service: txn_client,
            txn: None,
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // retry + backoff
        for i in 0..RETRY_TIMES {
            let res =
                block_on(async move { self.tso_service.get_timestamp(&TimestampRequest {}).await });

            if let Ok(resp) = res {
                return Ok(resp.ts);
            }

            // Technically, if i == RETRY_TIME-1, no need to sleep. But this passes the test.
            thread::sleep(Duration::from_millis(
                u64::pow(2, i as u32) * BACKOFF_TIME_MS,
            ));
        }

        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        assert!(self.txn.is_none());

        self.txn = Some(Txn {
            start_ts: self.get_timestamp().unwrap(),
            writes: vec![],
        });

        clog!("begin(start_ts={})", self.txn.as_ref().unwrap().start_ts);
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        clog!("get(k={})", u8_vec_to_str(&key));

        let res = block_on(async {
            self.txn_service
                .get(&GetRequest {
                    key: key.clone(),
                    start_ts: self.txn.as_ref().unwrap().start_ts,
                })
                .await
        })
        .unwrap();

        clog!(
            "get(k={}) -> {}",
            u8_vec_to_str(&key),
            u8_vec_to_str(&res.value)
        );

        Ok(res.value)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        clog!(
            "set(k={}, v={})",
            u8_vec_to_str(&key),
            u8_vec_to_str(&value)
        );

        self.txn.as_mut().unwrap().writes.push(Write { key, value });
    }

    /// Commits a transaction.
    pub fn commit(&mut self) -> Result<bool> {
        let start_ts = self.txn.as_ref().unwrap().start_ts;

        clog!("commit(start_ts={}) begins", start_ts);

        let primary = &self.txn.as_ref().unwrap().writes[0];

        // prewrite all writes (primary + secondaries)
        for w in self.txn.as_ref().unwrap().writes.iter() {
            let prewrite_res = block_on(async {
                self.txn_service
                    .prewrite(&PrewriteRequest {
                        key: w.key.clone(),
                        value: w.value.clone(),
                        start_ts,
                        primary_key: primary.key.clone(),
                    })
                    .await
            });

            if prewrite_res.is_err() || !prewrite_res.as_ref().unwrap().ok {
                clog!(
                    "prewrite(k={}, v={}, start_ts={}, primary={}) failed",
                    u8_vec_to_str(&w.key),
                    u8_vec_to_str(&w.value),
                    start_ts,
                    u8_vec_to_str(&primary.key),
                );
                self.txn = None;
                return Ok(false);
            }
        }

        // generate commit_ts
        let commit_ts = self.get_timestamp().unwrap();
        clog!(
            "commit(start_ts={}) finished prewrites, commit_ts={}",
            start_ts,
            commit_ts
        );

        // commit primary + secondaries
        for (i, w) in self.txn.as_ref().unwrap().writes.iter().enumerate() {
            let is_primary = i == 0;

            let commit_write_res = block_on(async {
                self.txn_service
                    .commit(&CommitRequest {
                        is_primary,
                        key: w.key.clone(),
                        start_ts,
                        commit_ts,
                    })
                    .await
            });

            // Commit succeeds when the primary is committed.
            // If fail when commiting secondary, will be rolled-forward by other txns.
            if is_primary {
                if let Err(err) = commit_write_res {
                    self.txn = None;
                    if let Error::Other(reason) = err.clone() {
                        if reason == "reqhook" {
                            return Ok(false);
                        }
                    }
                    return Err(err);
                } else if !commit_write_res.unwrap().ok {
                    self.txn = None;
                    return Ok(false);
                }
            }
        }

        // set txn to None
        self.txn = None;

        clog!("commit(start_ts={}) succeeds", start_ts);

        Ok(true)
    }
}

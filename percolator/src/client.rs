use futures::executor::block_on;
use labrpc::*;
use std::{thread, time::Duration};

use crate::{
    msg::TimestampRequest,
    service::{TSOClient, TransactionClient},
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

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    tso: TSOClient,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client { tso: tso_client }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // retry + backoff
        for i in 0..RETRY_TIMES {
            let res = block_on(async move { self.tso.get_timestamp(&TimestampRequest {}).await });
            match res {
                Ok(resp) => return Ok(resp.ts),
                Err(_) => {}
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
        // Your code here.
        unimplemented!()
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        unimplemented!()
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        unimplemented!()
    }
}

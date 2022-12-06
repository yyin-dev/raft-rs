# raft-rs 

[Raft](https://raft.github.io/) (and a fault-tolerant key-value storage service) and [Percolator](https://research.google/pubs/pub36726/) implemented in Rust.

Based on [PingCAP Talent Plan - Distributed System in Rust](https://github.com/pingcap/talent-plan/blob/master/courses/dss/README.md).

## Roadmap

- [x] Lab 2
  - [x] Part 2A: leader election
  - [x] Part 2B: log replication
  - [x] Part 2C: persistence
  - [x] Part 2D: log compaction
- [x] Lab 3
  - [x] Part 3A: Key/value service without snapshots 
  - [x] Part 3B: Key/value service with snapshots
- [x] Percolator 

## Misc.

At the end of 3B, I tried [replace `prost` with `serde`](https://github.com/yinfredyue/raft-rs/commit/41f54dc66d00173d8255a9f9b1c949c97d551a95) for Raft snapshot serialization/deserialization. But for some reason this more than doubled CPU usage. I haven't figured out the cause, so I sticked with `prost`.



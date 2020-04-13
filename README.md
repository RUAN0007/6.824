# Overview
This repo includes my solution to the four lab assignments of [MIT 6.824 Distributed Systems Spring 2020](https://pdos.csail.mit.edu/6.824/index.html). All test cases are reliably passed, including non-credit challenges in Lab 4b. 

A few words about my design for Lab 4b, the sharded key-value storage with dynamic migration capability. 
* Each leader replica (as long as it believes itself to be a leader) periodically (for every 100ms) examimes whether it has unmigrated or unremoved shards. If not, it queries the shardmaster for Config C+1 given its current config C. If there exists Config C+1, the leader broadcasts this new Config for Raft consensus as an Op. 
  * **Unmigrated Shard**: Suppose S1 is assigned to Group G1 at Config C-1 and it is assigned to Group G2 at Config C. If G2 has _entered_ into Config C but not yet received shard migration request from G1 for S1, then S1 is an unmigrated for G2 at Config C. 
  * **Unremoved Shard**: Suppose S1 is assigned to Group G1 at Config C-1 and it is assigned to Group G2 at Config C. If G1 has _entered_ into Config C but not yet received shard removal request for S1 from G2, then S1 is an unremoved shard for G1 at Config C. 
* For each replica, when it commits a Config Op, it enters into this config by shifting its current config to this one. Then, based on the new config, it disables all future requests to shards that this replica group is no longer responsible. For each above shard, the replica invokes an Migration RPC asyncly to the leader replica in the responsible shard. 
* For each replica, when it commits a State Migration Op from its log for its current Config, it installs the shard state and invokes a shard removal RPC call to the leader replica of the source group asyncly. Then it replies for future response to this migrated shard.  
* For each replica, when it commits a Shard Removal Op from Raft, it permanently removes this shard state.
* To respond State Migration or Shard Removal RPC, proper deduplication is checked before organizing them into Raft Op and submitting them for consensus. Similar deduplication technique is also employed when sequentially processing them from the committed logs. 
* Client deduplication tables are seperately managed for each shard. And they are migrated and installed with the shard states in State Migration RPC call and log processing. And each client is required to increment different command sequences for requests to different shards
* Besides states and deduplication table for each shard, the persisted KV server state also includes the current config and shard readilness table, which determines whether the shard is ready to serve for requests. All are periodically saved to snapshot during compaction 


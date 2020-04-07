package shardmaster

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32

	duplicatedTable      map[int64]int
	lastAppliedRaftIndex int
	lastAppliedRaftTerm  int
	latestTerm           int
	configs              []Config // indexed by config num
	cond                 *sync.Cond
}

type OpType int

const (
	move = iota
	leave
	join
	query
)

func (t OpType) String() string {
	strs := []string{"MOVE", "LEAVE", "JOIN", "QUERY"}
	return strs[t]
}

type Op struct {
	// Your data here.
	ClerkId int64
	CmdSeq  int

	Type    OpType
	Servers map[int][]string // for JoinArgs
	GIDs    []int            // for LeaveArgs
	Shard   int              // for MoveArgs
	GID     int              // for MoveArgs
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.log("Receive Request %s. ", args.String())
	defer func() {
		sm.log("Reply with %s", reply.String())
	}()
	clerkID := args.CmdID.ClientID
	cmdSeq := args.CmdID.CmdSeq
	reply.CmdID.ClientID = clerkID
	reply.CmdID.CmdSeq = cmdSeq
	op := Op{ClerkId: clerkID, CmdSeq: cmdSeq, Type: join,
		Servers: args.Servers}

	succFn := func() {
		reply.Err = OK
		reply.WrongLeader = false
	}

	failFn := func(err Err, isWrongLeader bool) {
		reply.Err = err
		reply.WrongLeader = isWrongLeader
	}
	sm.genericOp(op, succFn, failFn)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.log("Receive Request %s. ", args.String())
	defer func() {
		sm.log("Reply with %s", reply.String())
	}()
	clerkID := args.CmdID.ClientID
	cmdSeq := args.CmdID.CmdSeq
	reply.CmdID.ClientID = clerkID
	reply.CmdID.CmdSeq = cmdSeq
	op := Op{ClerkId: clerkID, CmdSeq: cmdSeq, Type: leave,
		GIDs: args.GIDs}

	succFn := func() {
		reply.Err = OK
		reply.WrongLeader = false
	}

	failFn := func(err Err, isWrongLeader bool) {
		reply.Err = err
		reply.WrongLeader = isWrongLeader
	}
	sm.genericOp(op, succFn, failFn)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.log("Receive Request %s. ", args.String())
	defer func() {
		sm.log("Reply with %s", reply.String())
	}()

	clerkID := args.CmdID.ClientID
	cmdSeq := args.CmdID.CmdSeq
	reply.CmdID.ClientID = clerkID
	reply.CmdID.CmdSeq = cmdSeq
	op := Op{ClerkId: clerkID, CmdSeq: cmdSeq, Type: move,
		Shard: args.Shard, GID: args.GID}

	succFn := func() {
		reply.Err = OK
		reply.WrongLeader = false
	}

	failFn := func(err Err, isWrongLeader bool) {
		reply.Err = err
		reply.WrongLeader = isWrongLeader
	}
	sm.genericOp(op, succFn, failFn)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.log("Receive Request %s. ", args.String())
	defer func() {
		sm.log("Reply with %s", reply.String())
	}()
	clerkID := args.CmdID.ClientID
	cmdSeq := args.CmdID.CmdSeq
	reply.CmdID.ClientID = clerkID
	reply.CmdID.CmdSeq = cmdSeq

	succFn := func() {
		if args.Num != -1 && args.Num < len(sm.configs) {
			reply.Err = OK
			reply.WrongLeader = false
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Err = OK
			reply.WrongLeader = false
			reply.Config = sm.configs[len(sm.configs)-1]
		}
	}

	failFn := func(err Err, isWrongLeader bool) {
		reply.Err = err
		reply.WrongLeader = isWrongLeader
	}
	op := Op{ClerkId: clerkID, CmdSeq: cmdSeq, Type: query}
	sm.genericOp(op, succFn, failFn)
}

// succ and fail are executed with the lock
func (sm *ShardMaster) genericOp(op Op, succ func(), fail func(Err, bool)) {
	sm.mu.RLock()
	if lastAppliedSeq, ok := sm.duplicatedTable[op.ClerkId]; ok && op.CmdSeq <= lastAppliedSeq {
		succ()
		sm.mu.RUnlock()
		return
	}
	sm.mu.RUnlock()

	var index, term int
	var isLeader bool
	if index, term, isLeader = sm.rf.Start(op); !isLeader {
		fail("Fail to submit cmd", true)
		return
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for {
		sm.cond.Wait()
		if lastAppliedSeq, ok := sm.duplicatedTable[op.ClerkId]; ok && op.CmdSeq <= lastAppliedSeq {
			succ()
			return
		} else if index <= sm.lastAppliedRaftIndex || term < sm.lastAppliedRaftTerm || term < sm.latestTerm {
			fail("Leadership changes", true)
			return
		} else {
			// do nothing, wait for next iteration of the loop, awaked by the sm.cond
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sm.dead, 1)
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

func (sm *ShardMaster) log(format string, a ...interface{}) {

	if !sm.killed() {
		var args []interface{}
		args = append(args, sm.me) // rf.me is constant, safe for concurrent read
		args = append(args, a...)
		DPrintf("(ShardMaster %d) "+format, args...)
	}
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) applyOpWithLock(op *Op) {
	sm.duplicatedTable[op.ClerkId] = op.CmdSeq
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{}
	newConfig.Num = lastConfig.Num + 1
	newConfig.Groups = make(map[int][]string, 0)
	defer func() {
		if op.Type == query {
			// sm.log("Apply Op %s. \n\tPrev Config: %s", op.Type.String(), lastConfig.String())
		} else {
			sm.log("Apply Op %s. \n\tPrev Config: %s \n\tNew Config: %s", op.Type.String(), lastConfig.String(), newConfig.String())
		}
	}()
	if op.Type == move {
		for gid, servers := range lastConfig.Groups {
			newConfig.Groups[gid] = servers
		}
		for shardId, gid := range lastConfig.Shards {
			if shardId == op.Shard {
				newConfig.Shards[shardId] = op.GID
			} else {
				newConfig.Shards[shardId] = gid
			}
		}
		sm.configs = append(sm.configs, newConfig)
	} else if op.Type == leave {
		// The # of assigned shard per live group
		gidShardCount := map[int]int{}
		// The id of the live group, which is not supposed to remove
		// This slice will be later sorted based on the assigned shard number in previous confif
		var liveGids []int
		for gid, servers := range lastConfig.Groups {
			live := true
			for _, leftGid := range op.GIDs {
				if gid == leftGid {
					live = false
					break
				} // end if
			} // end for leftGid

			if live {
				gidShardCount[gid] = 0
				newConfig.Groups[gid] = servers
				liveGids = append(liveGids, gid)
			}
		} // end for gid

		for _, gid := range lastConfig.Shards {
			if _, ok := gidShardCount[gid]; ok {
				gidShardCount[gid]++
			}
		}

		sort.Slice(liveGids, func(i, j int) bool {
			return gidShardCount[liveGids[i]] < gidShardCount[liveGids[j]]
		})
		i := 0
		for shardId, gid := range lastConfig.Shards {
			if _, ok := gidShardCount[gid]; ok {
				newConfig.Shards[shardId] = gid
			} else if len(liveGids) == 0 {
				newConfig.Shards[shardId] = 0
			} else {
				// schedule the obsolete shard to the live group
				// with the min # of assigned shard.
				// Do it in round robin manner for faireness
				newConfig.Shards[shardId] = liveGids[i%len(liveGids)]
				i++
			}
		} // end for

		sm.configs = append(sm.configs, newConfig)
	} else if op.Type == join {
		// the assigned shard for each prevGid
		gidShardCount := map[int]int{}
		var prevGids []int
		var newGids []int
		for gid, servers := range lastConfig.Groups {
			gidShardCount[gid] = 0
			newConfig.Groups[gid] = servers
			prevGids = append(prevGids, gid)
		} // end for gid

		for gid, servers := range op.Servers {
			if _, ok := newConfig.Groups[gid]; ok {
				panic(fmt.Sprintf("Joined group %d already exists", gid))
			}
			newConfig.Groups[gid] = servers
			newGids = append(newGids, gid)
		}
		for shardId, gid := range lastConfig.Shards {
			gidShardCount[gid]++
			newConfig.Shards[shardId] = gid
		}

		sort.Slice(prevGids, func(i, j int) bool {
			return gidShardCount[prevGids[i]] > gidShardCount[prevGids[j]]
		}) // preGids are sorted based upon the assigned shard count in DECREASING order.

		rescheduledShardCount := NShards / len(newConfig.Groups) * len(op.Servers)

		// Shed the shard from gid with the max # of shards to the new group in round robin manner
		for i := 0; i < rescheduledShardCount; i++ {
			fromGid := 0
			if 0 < len(prevGids) {
				fromGid = prevGids[i%len(prevGids)]
			}
			toGid := newGids[i%len(newGids)]
			for shardID, gid := range newConfig.Shards {
				if gid == fromGid {
					newConfig.Shards[shardID] = toGid
					break
				} // if
			} // end for
		}
		sm.configs = append(sm.configs, newConfig)
	} else if op.Type == query {
		// do nothing
	}
	return
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.duplicatedTable = make(map[int64]int)
	sm.cond = sync.NewCond(sm.mu.RLocker())
	sm.lastAppliedRaftIndex = 0
	sm.lastAppliedRaftTerm = 0
	sm.latestTerm = 0

	// Your code here.
	go func() {
		for appliedMsg := range sm.applyCh {
			// NOTE: the for loop is executed with holding rf.mu.
			// Be careful to call any rf.Method() which also acquires the rf.mu lock, leading to the deadlock.
			if sm.killed() {
				break
			}
			sm.mu.Lock()
			if appliedMsg.CommandIndex == 0 {
				// Ignore the first empty raft log
			} else if !appliedMsg.CommandValid {
				// Now it implies for a snapshot, which shall not occur in shard master, as we do not compact snapshots.
				panic("Receive invalid cmd...")
			} else if op, ok := appliedMsg.Command.(Op); ok {

				sm.log("Receive Committed Op from ClerkId %d, CmdSeq %d, Raft Index %d and Term %d", op.ClerkId, op.CmdSeq, appliedMsg.CommandIndex, appliedMsg.Term)

				if appliedCmdSeq, ok := sm.duplicatedTable[op.ClerkId]; !ok {
					if op.CmdSeq != 0 {
						panic(fmt.Sprintf("Clerk %d has sent Request %d before committing previous requests.", op.ClerkId, op.CmdSeq))
					}
					sm.applyOpWithLock(&op)
				} else if op.CmdSeq <= appliedCmdSeq {
					sm.log("Ignore duplicated request (clerkID: %d, cmdSeq: %d)", op.ClerkId, op.CmdSeq)
				} else if op.CmdSeq == appliedCmdSeq+1 {
					sm.applyOpWithLock(&op)
				} else {
					panic(fmt.Sprintf("Clerk %d has sent Request %d before committing previous requests.", op.ClerkId, op.CmdSeq))
				}
			} else {
				panic("Wrong recognized cmd op type...")
			}
			sm.lastAppliedRaftIndex = appliedMsg.CommandIndex
			sm.lastAppliedRaftTerm = appliedMsg.Term
			sm.cond.Broadcast()
			sm.mu.Unlock()
		} // end for
	}()
	go func() {
		// Periodically poll to detect the leadership changes
		for !sm.killed() {
			latestTerm, _ := sm.rf.GetState()
			sm.mu.Lock()
			sm.latestTerm = latestTerm
			sm.cond.Broadcast()
			sm.mu.Unlock()
			// This is to in case some requests has submitted to raft and get committed before the RPC hanlders calls the wait()
			// Suppose there is no further requests, this request will be blocked there.
			// Hence, we periodically awake them up to test for condition.
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return sm
}

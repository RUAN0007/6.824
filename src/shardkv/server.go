package shardkv

// import "../shardmaster"
import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const GetOp = "Get"
const PutOp = "Put"
const AppendOp = "Append"
const ConfigOp = "Config"
const MigrateOp = "Migrate"
const RemoveOp = "Remove"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string // "Get", Put" or "Append"

	Key   string
	Value string // empty if Op = Get
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	CmdSeq  int

	// For Shard Migration/Shard Removal
	FromGid       int
	ConfigNum     int
	ShardNum      int
	ShardState    map[string]string
	ShardDupTable map[int64]int

	// For Config Update
	Shards [shardmaster.NShards]int // shard -> gid
	Groups map[int][]string         // gid -> servers[]
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead           int32 // set by Kill()
	config         shardmaster.Config
	shardStates    [shardmaster.NShards]map[string]string
	shardDupTables [shardmaster.NShards]map[int64]int
	shardReady     [shardmaster.NShards]bool

	lastAppliedRaftIndex int
	lastAppliedRaftTerm  int
	latestTerm           int
	cond                 *sync.Cond
	persister            *raft.Persister
	mck                  *shardmaster.Clerk
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	reply.ClerkId = args.ClerkId
	reply.CmdSeq = args.CmdSeq
	shard := key2shard(args.Key)
	reason := ""
	kv.log("Receive Request %s for Shard %d. ", args.String(), shard)
	defer func() {
		kv.log("Reply with %s, reason = %s", reply.String(), reason)
	}()

	executed := func() bool {
		lastAppliedSeq, ok := kv.shardDupTables[shard][args.ClerkId]
		return ok && args.CmdSeq <= lastAppliedSeq
	}

	shardReady := func() Err {
		if kv.shardReady[shard] {
			return OK
		} else {
			return ErrWrongGroup
		}
	}

	succFn := func() {
		reason = fmt.Sprintf("Executed from Raft logs. Cmd Seq %d, Last Applied Cmd Seq %d for Shard %d", args.CmdSeq, kv.shardDupTables[shard][args.ClerkId], shard)
		if val, ok := kv.shardStates[shard][args.Key]; ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	}

	kv.mu.RLock()
	if err := shardReady(); err != OK {
		reply.Err = ErrWrongGroup
		reason = fmt.Sprintf("Shard %d not ready at start", shard)
		kv.mu.RUnlock()
		return
	}

	if executed() {
		succFn()
		// Override the reason set in succFn()
		reason = fmt.Sprintf("Already Executed at start. Cmd Seq %d, Last Applied Cmd Seq %d for Shard %d", args.CmdSeq, kv.shardDupTables[shard], shard)
		kv.mu.RUnlock()
		return
	}
	curConfigNum := kv.config.Num
	kv.mu.RUnlock()
	if opIndex, opTerm, isLeader := kv.rf.Start(Op{Key: args.Key, Op: GetOp, ClerkId: args.ClerkId, CmdSeq: args.CmdSeq}); isLeader {
		failFn := func(err Err) {
			if err == ErrWrongGroup {
				reason = fmt.Sprintf("Config changes from Config %d to %d. Shard %d no longer applies", curConfigNum, kv.config.Num, shard)
			} else if err == ErrWrongLeader {
				reason = fmt.Sprintf("Raft log bypasses (op index %d, term %d) (last index %d, last term %d, latest term %d).", opIndex, opTerm, kv.lastAppliedRaftIndex, kv.lastAppliedRaftTerm, kv.latestTerm)
			}
			reply.Err = err
		}
		kv.waitFor(opIndex, opTerm, executed, shardReady, succFn, failFn)
		return
	} else {
		reason = "Not the Raft leader"
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.ClerkId = args.ClerkId
	reply.CmdSeq = args.CmdSeq
	shard := key2shard(args.Key)
	reason := ""
	kv.log("Receive Request %s for Shard %d. ", args.String(), shard)
	defer func() {
		kv.log("Reply with %s, reason = %s", reply.String(), reason)
	}()

	executed := func() bool {
		var lastApplied int
		if lastAppliedSeq, ok := kv.shardDupTables[shard][args.ClerkId]; ok {
			lastApplied = lastAppliedSeq
		} else {
			lastApplied = -1
		}
		return args.CmdSeq <= lastApplied
	}

	shardReady := func() Err {
		if kv.shardReady[shard] {
			return OK
		} else {
			return ErrWrongGroup
		}
	}

	kv.mu.RLock()
	if err := shardReady(); err != OK {
		kv.mu.RUnlock()
		reply.Err = ErrWrongGroup
		reason = fmt.Sprintf("Shard %d not ready at start", shard)
		return
	}

	if executed() {
		reply.Err = OK
		// Put reason after succFn() to override
		reason = fmt.Sprintf("Executed at start. Cmd Seq %d, Last Applied Cmd Seq %d for Shard %d", args.CmdSeq, kv.shardDupTables[shard][args.ClerkId], shard)
		kv.mu.RUnlock() // protect the kv.shardDupTables for reason assignment
		return
	}
	curConfigNum := kv.config.Num
	kv.mu.RUnlock()
	if opIndex, opTerm, isLeader := kv.rf.Start(Op{Key: args.Key, Op: args.Op, Value: args.Value, ClerkId: args.ClerkId, CmdSeq: args.CmdSeq}); isLeader {
		succFn := func() {
			reason = fmt.Sprintf("Executed from Raft logs. Cmd Seq %d, Last Applied Cmd Seq %d for Shard %d", args.CmdSeq, kv.shardDupTables[shard], shard)
			reply.Err = OK
		}
		failFn := func(err Err) {
			if err == ErrWrongGroup {
				reason = fmt.Sprintf("Config changes from Config %d to %d. Shard %d no longer applies", curConfigNum, kv.config.Num, shard)
			} else if err == ErrWrongLeader {
				reason = fmt.Sprintf("Raft log bypasses (op index %d, term %d) (last index %d, last term %d, latest term %d).", opIndex, opTerm, kv.lastAppliedRaftIndex, kv.lastAppliedRaftTerm, kv.latestTerm)
			}
			reply.Err = err
		}

		kv.waitFor(opIndex, opTerm, executed, shardReady, succFn, failFn)
		return
	} else {
		reason = "Not the Raft leader"
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	// Your code here.
	reply.Uuid = args.Uuid
	kv.log("Receive Request %s. ", args.String())
	curConfigNum := 0
	reason := ""
	defer func() {
		kv.log("Reply with %s for Shard %d at Config %d, reason = %s", reply.String(), args.ShardNum, curConfigNum, reason)
	}()

	kv.mu.RLock()
	curConfigNum = kv.config.Num
	if args.ConfigNum < kv.config.Num {
		reply.Err = OK
		reason = fmt.Sprintf("Staled Migration Request for Config %d. Current Config %d", args.ConfigNum, kv.config.Num)
		kv.mu.RUnlock()
	} else if args.ConfigNum == kv.config.Num && kv.shardReady[args.ShardNum] {
		reply.Err = OK
		reason = fmt.Sprintf("Migration already installed under current Config %d", kv.config.Num)
		kv.mu.RUnlock()
	} else if args.ConfigNum == kv.config.Num && !kv.shardReady[args.ShardNum] {
		kv.mu.RUnlock()
		op := Op{Op: MigrateOp, ConfigNum: args.ConfigNum, ShardNum: args.ShardNum, FromGid: args.FromGid}
		op.ShardState = strMapCopy(args.State)
		op.ShardDupTable = intMapCopy(args.DupTable)

		if index, term, isLeader := kv.rf.Start(op); isLeader {
			isShardReady := func() bool {
				// if config num bypasses, the migration request at previous config must already take effect.
				// It is because only until shard migrations have finished then the leader can propose the later config change.
				return args.ConfigNum < kv.config.Num || kv.shardReady[args.ShardNum]
			}

			noFails := func() Err {
				return OK
			}

			succ := func() {
				reason = fmt.Sprintf("Shard %d ready", args.ShardNum)
				reply.Err = OK
			}

			fail := func(err Err) {
				reason = fmt.Sprintf("Raft log bypasses. Submitted index %d and term %d. Last applied index %d and term %d", index, term, kv.lastAppliedRaftIndex, kv.lastAppliedRaftTerm)
				reply.Err = err
			}
			kv.waitFor(index, term, isShardReady, noFails, succ, fail)

		} else {
			reason = "Raft not the leader."
			reply.Err = ErrWrongLeader
		}
	} else if args.ConfigNum > kv.config.Num {
		kv.mu.RUnlock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			// It might be a staled leader
			// However, since it is contacted in the network,
			// it will soon realize a higher leader and step down to a follower.
			// The invoker of this RPC will retry and realize that it no longer to be the leader and change to another replica to call.s
			reason = fmt.Sprintf("Unreached config. Current Config %d. Migration Config %d", curConfigNum, args.ConfigNum)
			reply.Err = ErrUnreachedConfig
		} else {
			reason = fmt.Sprintf("Staled current Config %d. Migration Config %d. Wrong Leader", curConfigNum, args.ConfigNum)
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *ShardKV) RemoveShard(args *RemoveShardArgs, reply *RemoveShardReply) {
	// Your code here.
	reply.Uuid = args.Uuid
	kv.log("Receive Request %s. ", args.String())
	curConfigNum := 0
	reason := ""
	defer func() {
		kv.log("Reply with %s for Shard %d at Config %d, reason = %s ", reply.String(), args.ShardNum, curConfigNum, reason)
	}()
	kv.mu.RLock()
	curConfigNum = kv.config.Num
	if args.ConfigNum < kv.config.Num {
		reply.Err = OK
		reason = fmt.Sprintf("Staled Shard %d Removal Request for Config %d. Current Config %d", args.ShardNum, args.ConfigNum, kv.config.Num)
		kv.mu.RUnlock()
	} else if args.ConfigNum == kv.config.Num && len(kv.shardStates[args.ShardNum]) == 0 {
		reply.Err = OK
		reason = fmt.Sprintf("Shard %d already removed under current Config %d", args.ShardNum, kv.config.Num)
		kv.mu.RUnlock()
		return
	} else if args.ConfigNum == kv.config.Num && len(kv.shardStates[args.ShardNum]) != 0 {
		kv.mu.RUnlock()
		op := Op{Op: RemoveOp, ShardNum: args.ShardNum, ConfigNum: args.ConfigNum}
		if index, term, isLeader := kv.rf.Start(op); isLeader {
			isShardRemoved := func() bool {
				// if config num bypasses, the shard removal request at previous config must already take effect.
				// It is because only until shard migrations have finished then the leader can propose the later config change.
				return args.ConfigNum < kv.config.Num || len(kv.shardStates[args.ShardNum]) == 0
			}

			noFails := func() Err {
				return OK
			}

			succ := func() {
				reason = fmt.Sprintf("Shard %d removed.", args.ShardNum)
				reply.Err = OK
			}

			fail := func(err Err) {
				reason = fmt.Sprintf("Raft log bypasses. Submitted index %d and term %d. Last applied index %d and term %d", index, term, kv.lastAppliedRaftIndex, kv.lastAppliedRaftTerm)
				reply.Err = err
			}
			kv.waitFor(index, term, isShardRemoved, noFails, succ, fail)

		} else {
			reply.Err = ErrWrongLeader
			reason = "Raft not the leader."
		}
		return
	} else if args.ConfigNum > kv.config.Num {
		// Due to the causality, there must be another leader under Config args.ConfigNum, this replica must be stale.
		reply.Err = ErrWrongLeader
		reason = fmt.Sprintf("Staled current Config %d. Migration Config %d. Wrong Leader", kv.config.Num, args.ConfigNum)
		kv.mu.RUnlock()
		return
	}
}

func (kv *ShardKV) waitFor(opIndex, opTerm int, succConditionWithLock func() bool, failConditionWithLock func() Err, succ func(), fail func(Err)) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	for {
		kv.cond.Wait()
		if err := failConditionWithLock(); err != OK {
			fail(err)
			break
		}
		if succConditionWithLock() {
			succ()
			break
		}
		if opIndex <= kv.lastAppliedRaftIndex || opTerm < kv.lastAppliedRaftTerm || opTerm < kv.latestTerm {
			fail(ErrWrongLeader)
			break
		}
	}
}

// invokeMigrationWithoutLock will make RPC call to the target group to transition shard shardNum, based on Config configNum.
func (kv *ShardKV) invokeMigrationWithoutLock(args MigrateArgs, serverNames []string) {

	numReplica := len(serverNames)
	si := 0
	for {
		server := kv.make_end(serverNames[si])
		var reply MigrateReply
		ok := server.Call("ShardKV.Migrate", &args, &reply)
		if ok && reply.Uuid != args.Uuid {
			// Network reorder. Try on the same server.
			continue
		} else if ok && reply.Err == OK {
			return
		} else if ok && reply.Err == ErrUnreachedConfig {
			time.Sleep(50 * time.Millisecond)
		} else if !ok || reply.Err == ErrWrongLeader {
			// This server is down. Try on another server
			si++
			si %= numReplica
		} else {
			kv.panic("Unrecognized error %v", reply.Err)
		}
	}
}

func (kv *ShardKV) invokeShardRemovalWithoutLock(args RemoveShardArgs, serverNames []string) {

	si := 0
	numReplica := len(serverNames)
	for {
		server := kv.make_end(serverNames[si])
		var reply RemoveShardReply
		ok := server.Call("ShardKV.RemoveShard", &args, &reply)
		if ok && reply.Uuid != args.Uuid {
			// Network reorder. Try on the same server.
			continue
		} else if ok && reply.Err == OK {
			return
		} else if !ok || reply.Err == ErrWrongLeader {
			// This server is down. Try on another server
			si++
			si %= numReplica
		} else {
			kv.panic("Unrecognized error %v", reply.Err)
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) panic(format string, a ...interface{}) {
	var args []interface{}
	args = append(args, kv.gid, kv.me)
	args = append(args, a...)
	panic(fmt.Sprintf("(Group %d ShardKV %d) "+format, args...))
}

func (kv *ShardKV) log(format string, a ...interface{}) {

	if !kv.killed() {
		var args []interface{}
		args = append(args, kv.gid, kv.me)
		args = append(args, a...)
		DPrintf("(Group %d ShardKV %d) "+format, args...)
	}
}

func (kv *ShardKV) logOnlyOne(format string, a ...interface{}) {

	if !kv.killed() && kv.me == 0 {
		var args []interface{}
		args = append(args, kv.gid, kv.me)
		args = append(args, a...)
		DPrintf("(Group %d ShardKV %d) "+format, args...)
	}
}

func (kv *ShardKV) isDupCmd(shard int, clerkID int64, cmdSeq int) (info string, dup bool) {
	if appliedCmdSeq, ok := kv.shardDupTables[shard][clerkID]; !ok {
		if cmdSeq != 0 {
			kv.panic("Clerk %d has sent Request %d before committing previous requests.", clerkID, cmdSeq)
		}
		return "", false
	} else if cmdSeq <= appliedCmdSeq {
		// kv.log("Ignore duplicated request (clerkID: %d, cmdSeq: %d)", clerkID, cmdSeq)
		return fmt.Sprintf("CmdSeq: %d < Dup Table: %d", cmdSeq, appliedCmdSeq), true
	} else if cmdSeq == appliedCmdSeq+1 {
		return "", false
	} else {
		kv.panic("Clerk %d has sent Request %d before committing previous requests.", clerkID, cmdSeq)
	}
	return
}

func (kv *ShardKV) applyNewConfigWithLock(newConfigNum int, newShards [shardmaster.NShards]int, newGroups map[int][]string) (info []string) {
	shededShardGroup := map[int]int{} // previous owned, now not owned by this kv.gid
	for shardID, oldGid := range kv.config.Shards {
		if oldGid == kv.gid && newShards[shardID] != kv.gid {
			shededShardGroup[shardID] = newShards[shardID]
		}
	}
	oldConfigDescription := kv.config.String()

	kv.config.Num = newConfigNum
	for shardID := range newShards {
		preGid := kv.config.Shards[shardID]
		kv.config.Shards[shardID] = newShards[shardID]
		if preGid == 0 && kv.config.Shards[shardID] == kv.gid {
			// No previous group owns Shard shardID ,Initialize them right away
			kv.shardStates[shardID] = map[string]string{}
			kv.shardDupTables[shardID] = map[int64]int{}
			kv.shardReady[shardID] = true
		}
	}

	for gid, serverNames := range newGroups {
		kv.config.Groups[gid] = []string{}
		for _, serverName := range serverNames {
			kv.config.Groups[gid] = append(kv.config.Groups[gid], serverName)
		}
	}
	newConfigDescription := kv.config.String()
	info = append(info, fmt.Sprintf("Shift Config from %s to %s", oldConfigDescription, newConfigDescription))

	for shardID, newGid := range shededShardGroup {
		kv.shardReady[shardID] = false
		args := MigrateArgs{}
		args.DupTable = intMapCopy(kv.shardDupTables[shardID])
		args.State = strMapCopy(kv.shardStates[shardID])
		args.ShardNum = shardID
		args.ConfigNum = newConfigNum
		args.FromGid = kv.gid
		args.Uuid = nrand()
		serverNames := kv.config.Groups[newGid]
		// Call RPC async without holding the lock.
		go kv.invokeMigrationWithoutLock(args, serverNames)
		info = append(info, fmt.Sprintf("Turn off Shard %d. Invoke Migration Request to Gid %d at Config %d", shardID, newGid, newConfigNum))
	}

	readyStr := "Current Shard Ready: "
	for shardID, ready := range kv.shardReady {
		readyStr += fmt.Sprintf("%d=%t, ", shardID, ready)
	}
	info = append(info, readyStr)
	return
}

func (kv *ShardKV) applyOpWithLock(op *Op) (info []string) {

	if op.Op == GetOp {
		shard := key2shard(op.Key)
		info = append(info, fmt.Sprintf("Get Key: %s, Shard: %d, ClerkID: %d, CmSeq: %d", op.Key, op.ShardNum, op.ClerkId, op.CmdSeq))
		if !kv.shardReady[shard] {
			info = append(info, "WrongShardGroup")
		} else if dInfo, dup := kv.isDupCmd(shard, op.ClerkId, op.CmdSeq); dup {
			info = append(info, dInfo)
		} else {
			kv.shardDupTables[shard][op.ClerkId] = op.CmdSeq
			info = append(info, "Executed")
		}
	} else if op.Op == PutOp {
		shard := key2shard(op.Key)
		info = append(info, fmt.Sprintf("Put Key: %s, Shard: %d, Val: %s, ClerkID: %d, CmSeq: %d", op.Key, op.ShardNum, op.Value, op.ClerkId, op.CmdSeq))

		if !kv.shardReady[shard] {
			info = append(info, "WrongShardGroup")
		} else if dInfo, dup := kv.isDupCmd(shard, op.ClerkId, op.CmdSeq); dup {
			info = append(info, dInfo)
		} else {
			kv.shardDupTables[shard][op.ClerkId] = op.CmdSeq
			kv.shardStates[shard][op.Key] = op.Value
			info = append(info, fmt.Sprintf("Put key=%s to val=%s", op.Key, op.Value))
		}
	} else if op.Op == AppendOp {
		shard := key2shard(op.Key)
		info = append(info, fmt.Sprintf("Append Key: %s, Shard: %d, Val: %s, ClerkID: %d, CmSeq: %d", op.Key, op.ShardNum, op.Value, op.ClerkId, op.CmdSeq))

		if !kv.shardReady[shard] {
			info = append(info, "WrongShardGroup")
		} else if dInfo, dup := kv.isDupCmd(shard, op.ClerkId, op.CmdSeq); dup {
			info = append(info, dInfo)
		} else if val, ok := kv.shardStates[shard][op.Key]; ok {
			kv.shardDupTables[shard][op.ClerkId] = op.CmdSeq
			kv.shardStates[shard][op.Key] += op.Value
			info = append(info, fmt.Sprintf("Append key=%s from val=%s to val=%s", op.Key, val, kv.shardStates[shard][op.Key]))
		} else {
			kv.shardDupTables[shard][op.ClerkId] = op.CmdSeq
			kv.shardStates[shard][op.Key] = op.Value
			info = append(info, fmt.Sprintf("Append key=%s from val='' to val=%s", op.Key, op.Value))
		}
	} else if op.Op == ConfigOp {
		if kv.config.Num+1 == op.ConfigNum {
			info = kv.applyNewConfigWithLock(op.ConfigNum, op.Shards, op.Groups)
		} else if kv.config.Num+1 < op.ConfigNum {
			kv.panic("Encounter gap between config. Current Config Num: %d. New Config Num: %d", kv.config.Num, op.ConfigNum)
		} else {
			// Ignore duplicates of config
			info = append(info, fmt.Sprintf("Duplicated. current config %d, op config %d", kv.config.Num, op.ConfigNum))
		}
	} else if op.Op == MigrateOp {
		if kv.config.Num == op.ConfigNum && !kv.shardReady[op.ShardNum] {

			kv.shardReady[op.ShardNum] = true
			kv.shardStates[op.ShardNum] = strMapCopy(op.ShardState)
			kv.shardDupTables[op.ShardNum] = intMapCopy(op.ShardDupTable)
			serverNames := kv.config.Groups[op.FromGid]

			removeShardArgs := RemoveShardArgs{}
			removeShardArgs.Uuid = nrand()
			removeShardArgs.ShardNum = op.ShardNum
			removeShardArgs.ConfigNum = op.ConfigNum
			go kv.invokeShardRemovalWithoutLock(removeShardArgs, serverNames)
			info = append(info, fmt.Sprintf("Install migrated Shard %d from Gid %d for current Config %d", op.ShardNum, op.FromGid, op.ConfigNum))
			info = append(info, fmt.Sprintf("Invoke remove Shard %d to Gid %d", op.ShardNum, op.FromGid))
		} else if op.ConfigNum < kv.config.Num {
			info = append(info, fmt.Sprintf("Ignore an early migration request for Config %d. But the current Config Number is %d. ", op.ConfigNum, kv.config.Num))
		} else {
			info = append(info, fmt.Sprintf("Ignore migrated Shard %d from Gid %d for Config %d. Current Config %d, is shard ready %t", op.ShardNum, op.FromGid, op.ConfigNum, kv.config.Num, kv.shardReady[op.ShardNum]))
		}
	} else if op.Op == RemoveOp {
		emptyShardState := len(kv.shardStates[op.ShardNum]) == 0
		if kv.config.Num == op.ConfigNum && !emptyShardState {
			kv.shardStates[op.ShardNum] = map[string]string{}
			kv.shardDupTables[op.ShardNum] = map[int64]int{}
			info = append(info, fmt.Sprintf("Remove State and Dup Table for Shard %d for Config %d", op.ShardNum, op.ConfigNum))
		} else {
			info = append(info, fmt.Sprintf("Ignore duplicated requests for removing Shard %d for Config %d. Current Config %d, is shard states empty: %t", op.ShardNum, op.ConfigNum, kv.config.Num, emptyShardState))
		}
	} else {
		kv.panic("Unrecognized Op Type: %s", op.Op)
	}
	return
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.mu = sync.RWMutex{}
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.maxraftstate = maxraftstate
	kv.config.Num = 0
	kv.config.Groups = map[int][]string{}

	kv.dead = 0
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardStates[i] = map[string]string{}
		kv.shardDupTables[i] = map[int64]int{}
		kv.shardReady[i] = false
	}

	kv.cond = sync.NewCond(kv.mu.RLocker())
	kv.lastAppliedRaftIndex = 0
	kv.lastAppliedRaftTerm = 0
	kv.latestTerm = 0
	kv.persister = persister
	kv.mck = shardmaster.MakeClerk(kv.masters)

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	go func() {
		for appliedMsg := range kv.applyCh {
			// NOTE: the for loop is executed with holding rf.mu.
			// Be careful to call any rf.Method() which also acquires the rf.mu lock, leading to the deadlock.
			if kv.killed() {
				break
			}
			kv.mu.Lock()
			if appliedMsg.CommandIndex == 0 {
				// Ignore the first empty raft log
			} else if !appliedMsg.CommandValid {
				// Now it implies for a snapshot, load the snapshot
				snapshotData, lastIncludedIndex, lastIncludedTerm := kv.rf.ReadSnapshot()
				if lastIncludedIndex != appliedMsg.CommandIndex {
					kv.panic("Unmatched snapshot index (%d) with raft index (%d)", lastIncludedIndex, appliedMsg.CommandIndex)
				}

				if lastIncludedTerm != appliedMsg.Term {
					kv.panic("Unmatched snapshot term (%d) with raft term (%d)", lastIncludedTerm, appliedMsg.Term)
				}

				d := labgob.NewDecoder(bytes.NewBuffer(snapshotData))

				if err := d.Decode(&kv.config); err != nil {
					kv.panic("Fail to decode config due to err msg %s", err.Error())
				}
				if err := d.Decode(&kv.shardStates); err != nil {
					kv.panic("Fail to decode kv state due to err msg %s", err.Error())
				}
				if err := d.Decode(&kv.shardDupTables); err != nil {
					kv.panic("Fail to decode duplicated table due to err msg %s", err.Error())
				}
				if err := d.Decode(&kv.shardReady); err != nil {
					kv.panic("Fail to decode shard ready due to err msg %s", err.Error())
				}
			} else if op, ok := appliedMsg.Command.(Op); ok {

				info := fmt.Sprintf("Accept Committed Op type %s at Raft Index %d and Term %d:", op.Op, appliedMsg.CommandIndex, appliedMsg.Term)
				actions := kv.applyOpWithLock(&op)
				for _, action := range actions {
					info += "\n\t" + action
				}
				kv.log(info)

			} else {
				kv.panic("Wrong recognized cmd op type...")
			}
			kv.lastAppliedRaftIndex = appliedMsg.CommandIndex
			kv.lastAppliedRaftTerm = appliedMsg.Term
			kv.cond.Broadcast()
			kv.mu.Unlock()
		} // end for
	}()

	go func() {
		// Periodically poll to detect the raft leadership changes and config changes
		for !kv.killed() {

			latestTerm, isLeader := kv.rf.GetState()
			pullForNextConfig := false
			kv.mu.Lock()
			curConfigNum := kv.config.Num
			kv.latestTerm = latestTerm

			if isLeader {
				assignedShard := map[int]bool{}
				for shardID, gid := range kv.config.Shards {
					if gid == kv.gid {
						assignedShard[shardID] = true
					} // end if gid
				} // end for

				unmigratedShard := []int{}
				unremovedShard := []int{}
				for shardID := 0; shardID < shardmaster.NShards; shardID++ {
					if _, ok := assignedShard[shardID]; ok {
						if !kv.shardReady[shardID] {
							// there exists assigned shards not yet migrated.
							unmigratedShard = append(unmigratedShard, shardID)
						}
					} else {
						// there exists shedded shards not removed.
						if 0 < len(kv.shardStates[shardID]) {
							unremovedShard = append(unremovedShard, shardID)
						}
					}
				}

				if len(unmigratedShard)+len(unremovedShard) > 0 {
					kv.log("There exists unmigrated shards %v or unremoved shard %v for current Config %d. Assigned shards [%v] for this gid %d. Not pull for new config", unmigratedShard, unremovedShard, curConfigNum, assignedShard, kv.gid)
				} else {
					pullForNextConfig = true // do it without lock later
				}
			}
			kv.cond.Broadcast() // this is to broadcast the latest term
			kv.mu.Unlock()
			if pullForNextConfig {
				nextConfig := kv.mck.Query(curConfigNum + 1)
				if curConfigNum < nextConfig.Num {
					// there might be gap btw curConfigNum and nextConfig.Num due to that this replica is a staled leader.
					// Nevertheless, we still submit the config cmd so that it will be de-duplicated.

					op := Op{Op: ConfigOp, ConfigNum: nextConfig.Num}
					for i := 0; i < shardmaster.NShards; i++ {
						op.Shards[i] = nextConfig.Shards[i]
					}
					op.Groups = make(map[int][]string, 0)
					for gid, serverNames := range nextConfig.Groups {
						op.Groups[gid] = nil
						for _, serverName := range serverNames {
							op.Groups[gid] = append(op.Groups[gid], serverName)
						}
					}
					kv.log("At current Config %d, broadcast for new Config %d with Shard Arrangement %v", curConfigNum, op.ConfigNum, op.Shards)
					kv.rf.Start(op)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	go func() {
		// Periodically examine thr persisted states and save the snapshot if necessary.
		for !kv.killed() {
			if prevStateSize := kv.persister.RaftStateSize(); 0 < kv.maxraftstate && kv.maxraftstate < prevStateSize {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)

				kv.mu.RLock()
				e.Encode(kv.config)
				e.Encode(kv.shardStates)
				e.Encode(kv.shardDupTables)
				e.Encode(kv.shardReady)
				lastIncludedIndex := kv.lastAppliedRaftIndex
				lastIncludedTerm := kv.lastAppliedRaftTerm
				kv.mu.RUnlock()

				data := w.Bytes()

				kv.rf.SaveSnapshot(data, lastIncludedIndex, lastIncludedTerm)
				// kv.log("Save the states and dup table to Snapshot for lastIncluded index %d and term %d. (Prev State Size: %d, current Size: %d", lastIncludedIndex, lastIncludedTerm, prevStateSize, kv.persister.RaftStateSize())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return kv
}

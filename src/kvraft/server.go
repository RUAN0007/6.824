package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

var Log int = 0

const commitLogFormat = "/tmp/server-commit-%d.log"
const reqLogFormath = "/tmp/server-req-%d.log"

// To log the server action, set Log=1 and call this function before all execution.
func PrepareServerLogs(numServer int) {
	Log = 1
	for i := 0; i < numServer; i++ {
		commitLogPath := fmt.Sprintf(commitLogFormat, i)
		reqLogPath := fmt.Sprintf(reqLogFormath, i)
		var err error

		err = os.Remove(commitLogPath)
		if !os.IsNotExist(err) && err != nil {
			panic(fmt.Sprintf("Fail to remove %s with err msg %s", commitLogPath, err.Error()))
		}

		err = os.Remove(reqLogPath)
		if !os.IsNotExist(err) && err != nil {
			panic(fmt.Sprintf("Fail to remove %s with err msg %s", reqLogPath, err.Error()))
		}

		if _, err = os.Stat(commitLogPath); os.IsNotExist(err) {
			if _, err := os.Create(commitLogPath); err != nil {
				panic(fmt.Sprintf("Fail to create empty file %s with err msg %s", commitLogPath, err.Error()))
			}
		} else {
			panic("Should not reach here")
		}

		if _, err = os.Stat(reqLogPath); os.IsNotExist(err) {
			if _, err := os.Create(reqLogPath); err != nil {
				panic(fmt.Sprintf("Fail to create empty file %s with err msg %s", reqLogPath, err.Error()))
			}
		} else {
			panic("Should not reach here")
		}
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string // empty if Op = Get
	Op    string // "Get", Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	CmdSeq  int
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	duplicatedTable      map[int64]int // keeps the latest applied cmd seq from each clerk/client
	state                map[string]string
	lastAppliedRaftIndex int
	lastAppliedRaftTerm  int
	latestTerm           int
	cond                 *sync.Cond

	commitLogFile *os.File
	reqLogFile    *os.File
	persister     *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.ClerkId = args.ClerkId
	reply.CmdSeq = args.CmdSeq
	kv.log("Receive Request %s. ", args.String())
	kv.mu.RLock()
	defer func() {
		kv.log("Reply with %s", reply.String())
	}()
	if lastAppliedSeq, ok := kv.duplicatedTable[args.ClerkId]; ok && args.CmdSeq <= lastAppliedSeq {
		// Suppose this get request is located at index N. But another cmd at N+1 modifies the same key.
		// Even though lastAppliedSeq = N+2, we still satisfy linearizability
		// if returning the state reflecting the update at N+1th cmd.
		if val, ok := kv.state[args.Key]; ok { // the request has been committed in the index
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}

		action := fmt.Sprintf("Reply Get key=[%s] val=[%s] for clerk %d cmdseq %d after raft index %d and term %d\n", args.Key, reply.Value, args.ClerkId, args.CmdSeq, kv.lastAppliedRaftIndex, kv.lastAppliedRaftTerm)
		kv.WriteReqLog(action)

		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	var index, term int
	var isLeader bool
	if index, term, isLeader = kv.rf.Start(Op{Key: args.Key, Op: "Get", ClerkId: args.ClerkId, CmdSeq: args.CmdSeq}); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	for {
		kv.log("Wait for the cmd for clerk %d, cmdSeq %d, raft index %d, term %d", args.ClerkId, args.CmdSeq, index, term)
		kv.cond.Wait()
		if lastAppliedSeq, ok := kv.duplicatedTable[args.ClerkId]; ok && args.CmdSeq <= lastAppliedSeq {
			if val, ok := kv.state[args.Key]; ok { // the request has been committed in the index
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}

			action := fmt.Sprintf("Reply Get key=[%s] val=[%s] for clerk %d cmdseq %d after raft index %d and term %d\n", args.Key, reply.Value, args.ClerkId, args.CmdSeq, kv.lastAppliedRaftIndex, kv.lastAppliedRaftTerm)
			kv.WriteReqLog(action)
			kv.log("Cmd for clerk %d and cmdSeq %d has committed. RUnlock and return", args.ClerkId, args.CmdSeq)
			return
		} else if index <= kv.lastAppliedRaftIndex {
			reply.Err = ErrWrongLeader
			kv.log("Cmd for clerk %d and cmdSeq %d fails. Raft Index bypasses", args.ClerkId, args.CmdSeq)
			return
		} else if term < kv.lastAppliedRaftTerm {
			reply.Err = ErrWrongLeader
			kv.log("Cmd for clerk %d and cmdSeq %d fails. Raft Term bypasses", args.ClerkId, args.CmdSeq)
			return
		} else if term < kv.latestTerm {
			// Detect the change of leadership
			// This cmd will probably be committed by the leader of the new term.
			// If so, we still ask the clerk to resend and hinge on the cmd de-deduplication.
			reply.Err = ErrWrongLeader
			kv.log("Cmd for clerk %d and cmdSeq %d may fail. Change of leadership", args.ClerkId, args.CmdSeq)
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.ClerkId = args.ClerkId
	reply.CmdSeq = args.CmdSeq
	kv.log("Receive Request %s. ", args.String())
	defer func() {
		kv.log("Reply with %s", reply.String())
	}()
	kv.mu.RLock()
	if lastAppliedSeq, ok := kv.duplicatedTable[args.ClerkId]; ok && args.CmdSeq <= lastAppliedSeq {
		reply.Err = OK

		action := fmt.Sprintf("Reply %s key=[%s] val=[%s] for clerk %d cmdseq %d after raft index %d and term %d\n", args.Op, args.Key, args.Value, args.ClerkId, args.CmdSeq, kv.lastAppliedRaftIndex, kv.lastAppliedRaftTerm)
		kv.WriteReqLog(action)

		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	var index, term int
	var isLeader bool
	if index, term, isLeader = kv.rf.Start(Op{Key: args.Key, Op: args.Op, Value: args.Value, ClerkId: args.ClerkId, CmdSeq: args.CmdSeq}); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	for {
		kv.log("Wait for the cmd for clerk %d,  cmdSeq %d, raft index %d, term %d", args.ClerkId, args.CmdSeq, index, term)
		kv.cond.Wait()
		if lastAppliedSeq, ok := kv.duplicatedTable[args.ClerkId]; ok && args.CmdSeq <= lastAppliedSeq {
			reply.Err = OK
			action := fmt.Sprintf("Reply %s key=[%s] val=[%s] for clerk %d cmdseq %d after raft index %d and term %d\n", args.Op, args.Key, args.Value, args.ClerkId, args.CmdSeq, kv.lastAppliedRaftIndex, kv.lastAppliedRaftTerm)
			kv.WriteReqLog(action)
			kv.log("Cmd for clerk %d and cmdSeq %d has committed. RUnlock and return", args.ClerkId, args.CmdSeq)
			return
		} else if index <= kv.lastAppliedRaftIndex {
			reply.Err = ErrWrongLeader
			kv.log("Cmd for clerk %d and cmdSeq %d fails. Raft Index bypasses", args.ClerkId, args.CmdSeq)
			return
		} else if term < kv.lastAppliedRaftTerm {
			reply.Err = ErrWrongLeader
			kv.log("Cmd for clerk %d and cmdSeq %d fails. Raft Term bypasses", args.ClerkId, args.CmdSeq)
			return
		} else if term < kv.latestTerm {
			// Detect the change of leadership
			// This cmd will probably be committed by the leader of the new term.
			// If so, we still ask the clerk to resend and hinge on the cmd de-deduplication.
			reply.Err = ErrWrongLeader
			kv.log("Cmd for clerk %d and cmdSeq %d may fail. Change of leadership", args.ClerkId, args.CmdSeq)
			return
		}
	}
}

func (kv *KVServer) log(format string, a ...interface{}) {

	// if _, isLeader := kv.rf.GetState(); !kv.killed() && isLeader {
	if !kv.killed() {
		var args []interface{}
		args = append(args, kv.me) // rf.me is constant, safe for concurrent read
		args = append(args, a...)
		DPrintf("(KVServer %d) "+format, args...)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyOpWithLock(op *Op) {
	kv.duplicatedTable[op.ClerkId] = op.CmdSeq
	action := ""
	if op.Op == "Put" { // the following clauses are conditioned on the correct cmdSeq
		kv.state[op.Key] = op.Value
		action = fmt.Sprintf("Put key=%s val=%s", op.Key, op.Value)
	} else if op.Op == "Append" {
		if val, ok := kv.state[op.Key]; ok {
			kv.state[op.Key] = val + op.Value
			action = fmt.Sprintf("Append key=[%s] pre-val=[%s] post-val=[%s]", op.Key, val, kv.state[op.Key])
		} else {
			kv.state[op.Key] = op.Value
			action = fmt.Sprintf("Append key=[%s] pre-val=[%s] post-val=[%s]", op.Key, "", kv.state[op.Key])
		}
	} else if op.Op == "Get" {
		action = fmt.Sprintf("Get key=%s", op.Key)
		// dothing
	} else {
		panic("Unrecognized op type")
	}
	kv.WriteCommitLog(fmt.Sprintf("\t%s\n", action))
}

func (kv *KVServer) WriteCommitLog(msg string) {
	if Log == 1 {
		if _, err := kv.commitLogFile.WriteString(msg); err != nil {
			panic(fmt.Sprintf("Can not write to commitLogFile for Server %d with err msg %s", kv.me, err.Error()))
		}
	}
}

func (kv *KVServer) WriteReqLog(msg string) {
	if Log == 1 {
		if _, err := kv.reqLogFile.WriteString(msg); err != nil {
			panic(fmt.Sprintf("Can not write to reqLogFile for Server %d with err msg %s", kv.me, err.Error()))
		}
	}
}

func (kv *KVServer) OpenLogFiles() {
	if Log == 1 {
		var err error
		commitLogPath := fmt.Sprintf(commitLogFormat, kv.me)
		reqLogPath := fmt.Sprintf(reqLogFormath, kv.me)
		if kv.commitLogFile, err = os.OpenFile(commitLogPath, os.O_APPEND|os.O_WRONLY, os.ModeAppend); err != nil {
			panic(fmt.Sprintf("Fail to open %s with err %s", commitLogPath, err.Error()))
		}
		if kv.reqLogFile, err = os.OpenFile(reqLogPath, os.O_APPEND|os.O_WRONLY, os.ModeAppend); err != nil {
			panic(fmt.Sprintf("Fail to open %s with err %s", reqLogPath, err.Error()))
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.mu = sync.RWMutex{}
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.duplicatedTable = make(map[int64]int)
	kv.state = make(map[string]string)
	kv.cond = sync.NewCond(kv.mu.RLocker())
	kv.lastAppliedRaftIndex = 0
	kv.lastAppliedRaftTerm = 0
	kv.latestTerm = 0
	kv.persister = persister
	kv.OpenLogFiles()

	// You may need initialization code here.
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
				// Now it implies for a snapshot, load the latest state from persister.
				// Note: the persister state can not be staled as we holding rf.mu lock.
				// the snapshot raft index and term will be updated later to lastAppliedRaftIndex/term
				snapshotData, lastIncludedIndex, lastIncludedTerm := kv.rf.ReadSnapshot()
				if lastIncludedIndex != appliedMsg.CommandIndex {
					panic(fmt.Sprintf("Unmatched snapshot index (%d) with raft index (%d)", lastIncludedIndex, appliedMsg.CommandIndex))
				}

				if lastIncludedTerm != appliedMsg.Term {
					panic(fmt.Sprintf("Unmatched snapshot term (%d) with raft term (%d)", lastIncludedTerm, appliedMsg.Term))
				}

				d := labgob.NewDecoder(bytes.NewBuffer(snapshotData))

				if err := d.Decode(&kv.state); err != nil {
					panic(fmt.Sprintf("Fail to decode kv state due to err msg %s", err.Error()))
				}
				if err := d.Decode(&kv.duplicatedTable); err != nil {
					panic(fmt.Sprintf("Fail to decode duplicated table due to err msg %s", err.Error()))
				}

			} else if op, ok := appliedMsg.Command.(Op); ok {

				kv.WriteCommitLog(fmt.Sprintf("%d) Raft Term: %d, ClerkId: %d, CmdSeq: %d\n", appliedMsg.CommandIndex, appliedMsg.Term, op.ClerkId, op.CmdSeq))
				kv.log("Receive Committed Op from ClerkId %d, CmdSeq %d, Raft Index %d and Term %d", op.ClerkId, op.CmdSeq, appliedMsg.CommandIndex, appliedMsg.Term)

				if appliedCmdSeq, ok := kv.duplicatedTable[op.ClerkId]; !ok {
					if op.CmdSeq != 0 {
						panic(fmt.Sprintf("Clerk %d has sent Request %d before committing previous requests.", op.ClerkId, op.CmdSeq))
					}
					kv.applyOpWithLock(&op)
				} else if op.CmdSeq <= appliedCmdSeq {
					kv.log("Ignore duplicated request (clerkID: %d, cmdSeq: %d)", op.ClerkId, op.CmdSeq)
					kv.WriteCommitLog("\tDuplicates")
				} else if op.CmdSeq == appliedCmdSeq+1 {
					kv.applyOpWithLock(&op)
				} else {
					panic(fmt.Sprintf("Clerk %d has sent Request %d before committing previous requests.", op.ClerkId, op.CmdSeq))
				}
			} else {
				panic("Wrong recognized cmd op type...")
			}
			kv.lastAppliedRaftIndex = appliedMsg.CommandIndex
			kv.lastAppliedRaftTerm = appliedMsg.Term
			kv.cond.Broadcast()
			kv.mu.Unlock()
		} // end for
	}()

	go func() {
		// Periodically poll to detect the leadership changes
		for !kv.killed() {
			latestTerm, _ := kv.rf.GetState()
			kv.mu.Lock()
			kv.latestTerm = latestTerm
			kv.cond.Broadcast()
			kv.mu.Unlock()
			// This is to in case some requests has submitted to raft and get committed before the RPC hanlders calls the wait()
			// Suppose there is no further requests, this request will be blocked there.
			// Hence, we periodically awake them up to test for condition.
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
				e.Encode(kv.state)
				e.Encode(kv.duplicatedTable)
				lastIncludedIndex := kv.lastAppliedRaftIndex
				lastIncludedTerm := kv.lastAppliedRaftTerm
				kv.mu.RUnlock()

				data := w.Bytes()

				kv.rf.SaveSnapshot(data, lastIncludedIndex, lastIncludedTerm)
				kv.log("Save the states and dup table to Snapshot for lastIncluded index %d and term %d. (Prev State Size: %d, current Size: %d", lastIncludedIndex, lastIncludedTerm, prevStateSize, kv.persister.RaftStateSize())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return kv
}

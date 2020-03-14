package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkID int64
	cmdSeq  int
	leader  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkID = nrand()
	ck.cmdSeq = 0
	ck.leader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) (result string) {
	// You will have to modify this function.
	args := GetArgs{key, ck.clerkID, ck.cmdSeq}
	reply := GetReply{}
	for {
		DPrintf("\n\n\nClerk %d sends request to server %d with request %s", ck.clerkID, ck.leader, args.String())
		if ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply); !ok {
			DPrintf("Clerk %d Get Request %d on Server %d fails", ck.clerkID, ck.cmdSeq, ck.leader)
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else if reply.ClerkId != args.ClerkId || reply.CmdSeq != args.CmdSeq {
			DPrintf("Detect the unmatched reply")
		} else if reply.Err == ErrWrongLeader {
			DPrintf("Clerk %d Get Request %d on Server %d encounters wrong leader", ck.clerkID, ck.cmdSeq, ck.leader)
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else if reply.Err == ErrNoKey {
			DPrintf("Clerk %d Get Request %d on Server %d encounters no key. ", ck.clerkID, ck.cmdSeq, ck.leader)
			result = ""
			ck.cmdSeq++
			return
		} else { //reply.Err == OK
			DPrintf("Clerk %d Get Request %d on Server %d gets Value %s. ", ck.clerkID, ck.cmdSeq, ck.leader, reply.Value)
			result = reply.Value
			ck.cmdSeq++
			return
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, ck.clerkID, ck.cmdSeq}
	reply := PutAppendReply{}
	for {
		DPrintf("\n\n\nClerk %d sends request to server %d with request %s", ck.clerkID, ck.leader, args.String())
		if ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply); !ok {
			DPrintf("Clerk %d PutAppend Request %d on Server %d fails", ck.clerkID, ck.cmdSeq, ck.leader)
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else if reply.ClerkId != args.ClerkId || reply.CmdSeq != args.CmdSeq {
			DPrintf("Detect the unmatched reply")
		} else if reply.Err == ErrWrongLeader {
			DPrintf("Clerk %d PutAppend Request %d on Server %d encounters wrong leader", ck.clerkID, ck.cmdSeq, ck.leader)
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else if reply.Err == ErrNoKey {
			DPrintf("Clerk %d PutAppend Request %d on Server %d encounters no key. ", ck.clerkID, ck.cmdSeq, ck.leader)
			panic("Should not reach here")
		} else { //reply.Err == OK
			DPrintf("Clerk %d PutAppend Request %d on Server %d succeeds. ", ck.clerkID, ck.cmdSeq, ck.leader)
			ck.cmdSeq++
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

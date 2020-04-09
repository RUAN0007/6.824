package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkID int64
	cmdSeq  int
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
	ck.clerkID = nrand() % 100000
	ck.cmdSeq = 0
	// Your code here.
	return ck
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.CmdID.ClientID = ck.clerkID
	args.CmdID.CmdSeq = ck.cmdSeq
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			matched := args.CmdID.ClientID == reply.CmdID.ClientID && args.CmdID.CmdSeq == reply.CmdID.CmdSeq
			if ok && reply.WrongLeader == false && matched {
				ck.cmdSeq++
				// Copy the config from reply to avoid race condtion
				result := Config{Num: reply.Config.Num}
				for i := 0; i < NShards; i++ {
					result.Shards[i] = reply.Config.Shards[i]
				}
				result.Groups = map[int][]string{}
				for gid, serverNames := range reply.Config.Groups {
					result.Groups[gid] = []string{}
					for _, serverName := range serverNames {
						result.Groups[gid] = append(result.Groups[gid], serverName)
					}
				}
				return result
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.CmdID.ClientID = ck.clerkID
	args.CmdID.CmdSeq = ck.cmdSeq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			matched := args.CmdID.ClientID == reply.CmdID.ClientID && args.CmdID.CmdSeq == reply.CmdID.CmdSeq
			if ok && reply.WrongLeader == false && matched {
				ck.cmdSeq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.CmdID.ClientID = ck.clerkID
	args.CmdID.CmdSeq = ck.cmdSeq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			matched := args.CmdID.ClientID == reply.CmdID.ClientID && args.CmdID.CmdSeq == reply.CmdID.CmdSeq
			if ok && reply.WrongLeader == false && matched {
				ck.cmdSeq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	args.CmdID.ClientID = ck.clerkID
	args.CmdID.CmdSeq = ck.cmdSeq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			matched := args.CmdID.ClientID == reply.CmdID.ClientID && args.CmdID.CmdSeq == reply.CmdID.CmdSeq
			if ok && reply.WrongLeader == false && matched {
				ck.cmdSeq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

package shardkv

import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrUnreachedConfig = "ErrUnreachedConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	CmdSeq  int
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("PutAppendArgs{Key: %s, Value: %s, Op: %s, ClerkId: %d, CmdSeq: %d}", args.Key, args.Value, args.Op, args.ClerkId, args.CmdSeq)
}

type PutAppendReply struct {
	Err     Err
	ClerkId int64
	CmdSeq  int
}

func (args *PutAppendReply) String() string {
	return fmt.Sprintf("PutAppendReply{Err: %s}", args.Err)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	CmdSeq  int
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("GetArgs{Key: %s, ClerkId: %d. CmdSeq: %d", args.Key, args.ClerkId, args.CmdSeq)
}

type GetReply struct {
	Err     Err
	Value   string
	ClerkId int64
	CmdSeq  int
}

func (args *GetReply) String() string {
	return fmt.Sprintf("GetReply{Err: %s, Value: %s, ClerkId: %d, CmdSeq: %d}", args.Err, args.Value, args.ClerkId, args.CmdSeq)
}

type MigrateArgs struct {
	DupTable  map[int64]int
	State     map[string]string
	ConfigNum int
	ShardNum  int
	FromGid   int
	Uuid      int64
}

func (args *MigrateArgs) String() string {
	return fmt.Sprintf("MigrateArgs{ConfigNum: %d, ShardNum: %d, FromGid; %d, DupTable: %v, Uuid: %d}", args.ConfigNum, args.ShardNum, args.FromGid, args.DupTable, args.Uuid)
}

type MigrateReply struct {
	Err  Err
	Uuid int64
}

func (args *MigrateReply) String() string {
	return fmt.Sprintf("MigrateReply{Err: %s, Uuid: %d}", args.Err, args.Uuid)
}

type RemoveShardArgs struct {
	ConfigNum int
	ShardNum  int
	Uuid      int64
}

func (args *RemoveShardArgs) String() string {
	return fmt.Sprintf("RemoveShardArgs{ConfigNum: %d, ShardNum: %d,  Uuid: %d}", args.ConfigNum, args.ShardNum, args.Uuid)
}

type RemoveShardReply struct {
	Err  Err
	Uuid int64
}

func (args *RemoveShardReply) String() string {
	return fmt.Sprintf("RemoveShardReply{Err: %s, Uuid: %d}", args.Err, args.Uuid)
}

func strMapCopy(src map[string]string) (dest map[string]string) {
	dest = make(map[string]string, len(src))
	for k, v := range src {
		dest[k] = v
	}
	return
}

func intMapCopy(src map[int64]int) (dest map[int64]int) {
	dest = make(map[int64]int, len(src))
	for k, v := range src {
		dest[k] = v
	}
	return
}

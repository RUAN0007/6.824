package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
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
	return fmt.Sprintf("GetReply{Err: %s, Value: %s}", args.Err, args.Value)
}

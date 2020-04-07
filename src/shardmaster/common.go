package shardmaster

import "fmt"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

const Debug = 0

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config *Config) String() string {
	return fmt.Sprintf("Config{Num: %d, Shards: %v, Groups: %v}", config.Num, config.Shards, config.Groups)
}

const (
	OK = "OK"
)

type Err string

type CmdID struct {
	ClientID int64
	CmdSeq   int
}

type JoinArgs struct {
	CmdID
	Servers map[int][]string // new GID -> servers mappings
}

func (args *JoinArgs) String() string {
	return fmt.Sprintf("JoinArgs{CmdID: (%d-%d), Servers: %v}", args.CmdID.ClientID, args.CmdID.CmdSeq, args.Servers)
}

type JoinReply struct {
	CmdID
	WrongLeader bool
	Err         Err
}

func (args *JoinReply) String() string {
	return fmt.Sprintf("JoinReply{CmdID: (%d-%d), WrongLeader: %t, Err: %s}", args.CmdID.ClientID, args.CmdID.CmdSeq, args.WrongLeader, args.Err)
}

type LeaveArgs struct {
	CmdID
	GIDs []int
}

func (args *LeaveArgs) String() string {
	return fmt.Sprintf("LeaveArgs{CmdID: (%d-%d), GIDs: %v}", args.CmdID.ClientID, args.CmdID.CmdSeq, args.GIDs)
}

type LeaveReply struct {
	CmdID
	WrongLeader bool
	Err         Err
}

func (args *LeaveReply) String() string {
	return fmt.Sprintf("LeaveReply{CmdID: (%d-%d), WrongLeader: %t, Err: %s}", args.CmdID.ClientID, args.CmdID.CmdSeq, args.WrongLeader, args.Err)
}

type MoveArgs struct {
	CmdID
	Shard int
	GID   int
}

func (args *MoveArgs) String() string {
	return fmt.Sprintf("MoveArgs{CmdID: (%d-%d), Shard: %d, GID: %d}", args.CmdID.ClientID, args.CmdID.CmdSeq, args.Shard, args.GID)
}

type MoveReply struct {
	CmdID
	WrongLeader bool
	Err         Err
}

func (args *MoveReply) String() string {
	return fmt.Sprintf("MoveReply{CmdID: (%d-%d), WrongLeader: %t, Err: %s}", args.CmdID.ClientID, args.CmdID.CmdSeq, args.WrongLeader, args.Err)
}

type QueryArgs struct {
	CmdID
	Num int // desired config number
}

func (args *QueryArgs) String() string {
	return fmt.Sprintf("QueryArgs{CmdID: (%d-%d),  Num: %d}", args.CmdID.ClientID, args.CmdID.CmdSeq, args.Num)
}

type QueryReply struct {
	CmdID
	WrongLeader bool
	Err         Err
	Config      Config
}

func (args *QueryReply) String() string {
	return fmt.Sprintf("QueryReply{CmdID: (%d-%d), WrongLeader: %t, Err: %s}", args.CmdID.ClientID, args.CmdID.CmdSeq, args.WrongLeader, args.Err)
}

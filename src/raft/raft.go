package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int
}

// index is in Msg
type RaftLog struct {
	Term int
	Msg  ApplyMsg
}

const (
	NA             = -1
	tickDurationMs = 10

	instantTick             = 1  // if timeoutTicks=instantTick, an immediate timeout is triggered on the next tick.
	electionTimeoutMinTicks = 40 // re-elect in a minimum of 400ms
	electionTimeoutMaxTicks = 60 // re-elect in a maximum of 600ms
	// heartbeatTicks          = 15 // send heartbeat in 150ms
	heartbeatTicks = 5 // send heartbeat in 20ms
)

const (
	Candidate = iota
	Follower
	Leader
)

func abbreviateLogs(logs []RaftLog) string {
	// Debug mode may lead to race condition, as this method references logs.
	// They are should be protected by the commitMu.
	if Debug == 0 {
		return ""
	}
	var logAbbr string
	if len(logs) == 0 {
		return "nil"
	} else {
		startTerm := logs[0].Term
		startIndex := logs[0].Msg.CommandIndex
		endIndex := startIndex
		for _, entry := range logs[1:] {
			if startTerm != entry.Term {
				logAbbr += fmt.Sprintf("(Term:%d)[%d:%d]", startTerm, startIndex, endIndex)
				startTerm = entry.Term
				startIndex = entry.Msg.CommandIndex
				endIndex = entry.Msg.CommandIndex
			} else {
				endIndex = entry.Msg.CommandIndex
			}
		} // end for
		logAbbr += fmt.Sprintf("(Term:%d)[%d:%d]", startTerm, startIndex, endIndex)
	} // end if
	return logAbbr
}

type Role int

func (r Role) String() string {
	return [...]string{"Candidate", "Follower", "Leader"}[r]
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	timeoutTicks int32

	// persistent
	role        Role
	currentTerm int
	votedFor    int
	logs        []RaftLog // The first log represents the last includedRaftIndex of the persisted snapshot
	// Its commandMsg.CommandValid = false

	// volatile on leaders
	nextIndex  []int
	matchIndex []int

	// volatile
	commitCond  *sync.Cond // Condition variable to signal when commitIdx is updated.
	commitIdx   int
	lastApplied int
	applyCh     chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// All assume holding the  lock
func (rf *Raft) logLen() int {
	if len(rf.logs) == 0 {
		panic(fmt.Sprintf("Empty log from Raft %d", rf.me))
	}
	baseIndex := rf.logs[0].Msg.CommandIndex
	return len(rf.logs) + baseIndex
}

func (rf *Raft) logAt(raftIndex int) *RaftLog {
	baseIndex := rf.logs[0].Msg.CommandIndex
	if raftIndex < baseIndex {
		panic(fmt.Sprintf("raft index (%d) < base index (%d) from server %d", raftIndex, baseIndex, rf.me))
	}
	return &rf.logs[raftIndex-baseIndex]
}

// Assume holding the mu
func (rf *Raft) String() string {
	if rf.role == Leader {
		return fmt.Sprintf("Role: %s, term: %d, votedFor: %d, logs: %s, nextIndex: %v, matchIndex: %v", rf.role.String(), rf.currentTerm, rf.votedFor, abbreviateLogs(rf.logs), rf.nextIndex, rf.matchIndex)
	} else {
		return fmt.Sprintf("Role: %s, term: %d, votedFor: %d, logs: %s", rf.role.String(), rf.currentTerm, rf.votedFor, abbreviateLogs(rf.logs))
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.role == Leader
	rf.mu.Unlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistWithLock() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.log("Persist currentTerm %d, votedFor %d, logs: %s", rf.currentTerm, rf.votedFor, abbreviateLogs(rf.logs))
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []RaftLog
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.logs) != nil {
		panic("Fail to decode the persisted states...")
	}
	rf.log("Initialize from persistence (currentTerm: %d, votedFor: %d, logs: %s", currentTerm, votedFor, abbreviateLogs(logs))
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVoteArgs{Term: %d, CandidateId: %d, LastLogIndex: %d, LastLogTerm: %d}",
		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

func (args *RequestVoteReply) String() string {
	return fmt.Sprintf("RequestVoteReply{VotedGranted: %t, Term: %d", args.VoteGranted, args.Term)
}

type AppendEntryArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLog
	LeaderCommit int
}

func (args *AppendEntryArgs) String() string {
	return fmt.Sprintf("AppendEntries{Term: %d, LeaderID: %d, PrevLogIndex: %d, PrevLogTerm: %d, Entries: %s, LeaderCommit: %d}", args.Term, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, abbreviateLogs(args.Entries), args.LeaderCommit)
}

type AppendEntryReply struct {
	Term            int
	Success         bool
	ConflictedTerm  int
	ConflictedIndex int
}

func (args *AppendEntryReply) String() string {
	return fmt.Sprintf("AppendReply{Term: %d, Success: %t, ConflictedTerm: %d, ConflictedIndex: %d}", args.Term, args.Success, args.ConflictedTerm, args.ConflictedIndex)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("InstallSnapshotArgs{Term: %d, LeaderID: %d, LastIncludedIndex: %d, LastIncludedTerm: %d}", args.Term, args.LeaderID, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (args *InstallSnapshotReply) String() string {
	return fmt.Sprintf("InstallSnapshotReply{Term: %d}", args.Term)
}

// Assume holding the lock
func (rf *Raft) CommitWithLock(commitIndex int) {
	if rf.commitIdx < commitIndex {
		rf.commitIdx = commitIndex
	}
	rf.commitCond.Broadcast()
}

func min(a, b int) (min int) {
	if a < b {
		min = a
	} else {
		min = b
	}
	return
}

func max(a, b int) (max int) {
	if a < b {
		max = b
	} else {
		max = a
	}
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	var actions []string
	beforeState := rf.String()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		actions = append(actions, "Ignore msg with smaller term.")
	} else {
		if rf.currentTerm < args.Term || rf.currentTerm == args.Term && rf.role == Candidate {
			actions = append(actions, rf.stepDownWithLock(args.Term))
		}
		reply.Term = rf.currentTerm
		rf.saveSnapshotWithLock(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
		rf.CommitWithLock(rf.logs[0].Msg.CommandIndex)
		atomic.StoreInt32(&rf.timeoutTicks, randomElectionTick())
	}
	actionStr := ""
	for _, action := range actions {
		actionStr += "\n\t" + action
	}
	rf.log("Receive from Server %d with request %s. Before State: %s, Reply with %s. Actions: %s ", args.LeaderID, args.String(), beforeState, reply.String(), actionStr)
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	baseIndex := rf.logs[0].Msg.CommandIndex
	beforeState := rf.String()
	var actions []string
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		actions = append(actions, "Ignore msg with smaller term.")
	} else {
		if rf.currentTerm < args.Term || rf.currentTerm == args.Term && rf.role == Candidate {
			actions = append(actions, rf.stepDownWithLock(args.Term))
		}
		// now rf.currentTerm == args.Term

		if firstEmptyLogIndex := rf.logLen(); firstEmptyLogIndex <= args.PrevLogIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictedIndex = firstEmptyLogIndex
			reply.ConflictedTerm = NA

			actions = append(actions, fmt.Sprintf("Fail to find the log at PrevlogIndex=%d", args.PrevLogIndex))

		} else if baseIndex <= args.PrevLogIndex && rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm

			reply.ConflictedIndex = baseIndex
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.logAt(i).Term < rf.logAt(args.PrevLogIndex).Term {
					reply.ConflictedIndex = i + 1
					break
				}
			}
			reply.ConflictedTerm = rf.logAt(args.PrevLogIndex).Term
			actions = append(actions, fmt.Sprintf("Find the inconsistent log at PrevlogIndex=%d (leader term: %d, follower term: %d)", args.PrevLogIndex, args.PrevLogTerm, rf.logAt(args.PrevLogIndex).Term))

			// delete the conflicted entries and all that follow it
			rf.logs = rf.logs[0 : args.PrevLogIndex-baseIndex]

		} else { // matched cases
			// Ignore previous logs before baseIndex as they are committed.

			effectivePrevLogIndex := max(args.PrevLogIndex, baseIndex)
			effectiveAppendedEntries := []RaftLog{}
			if effectivePrevLogIndex-args.PrevLogIndex < len(args.Entries) {
				effectiveAppendedEntries = args.Entries[effectivePrevLogIndex-args.PrevLogIndex:]
			}

			remainingIndex := effectivePrevLogIndex + len(effectiveAppendedEntries) + 1
			remainingEntries := []RaftLog{}
			if remainingIndex < rf.logLen() {
				remainingEntries = rf.logs[remainingIndex-baseIndex:]
			}

			rf.logs = append(rf.logs[0:effectivePrevLogIndex+1-baseIndex], effectiveAppendedEntries...)
			rf.logs = append(rf.logs, remainingEntries...)
			actions = append(actions, fmt.Sprintf("Keep the original log (inclusive) [%d:%d]", baseIndex, effectivePrevLogIndex))
			if 0 < len(effectiveAppendedEntries) {
				actions = append(actions, fmt.Sprintf("Append the entries (inclusive) [%d: %d]", effectivePrevLogIndex+1, effectivePrevLogIndex+len(effectiveAppendedEntries)))
			} else {
				actions = append(actions, "Append no entry")
			}
			if 0 < len(remainingEntries) {
				actions = append(actions, fmt.Sprintf("Keep the remaining logs (inclusive) [%d: %d]", remainingIndex, remainingIndex+len(remainingEntries)-1))
			} else {
				actions = append(actions, "Keep no remaining logs")
			}

			reply.Success = true
			reply.Term = rf.currentTerm
			reply.ConflictedIndex = NA
			reply.ConflictedTerm = NA
			commitIndex := min(args.LeaderCommit, remainingIndex-1)
			rf.CommitWithLock(commitIndex)
		}

		atomic.StoreInt32(&rf.timeoutTicks, randomElectionTick())
	}
	actionStr := ""
	for _, action := range actions {
		actionStr += "\n\t" + action
	}
	rf.log("Receive from Server %d with request %s. Before State: %s, Reply with %s. Actions: %s ", args.LeaderID, args.String(), beforeState, reply.String(), actionStr)
	rf.persistWithLock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	beforeState := rf.String()
	var actions []string
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		actions = append(actions, "Ignore msg with smaller term.")
	} else {
		if rf.currentTerm < args.Term {
			actions = append(actions, rf.stepDownWithLock(args.Term))
		}

		// now rf.currentTerm == args.Term
		validVotedFor := rf.votedFor == NA || rf.votedFor == args.CandidateId
		var upToDate bool
		lastLogIndex := rf.logLen() - 1
		if rf.logAt(lastLogIndex).Term < args.LastLogTerm {
			upToDate = true
		} else if rf.logAt(lastLogIndex).Term == args.LastLogTerm {
			upToDate = lastLogIndex <= args.LastLogIndex
		} else {
			upToDate = false
		}
		actions = append(actions, fmt.Sprintf("Check votedFor %t and up-to-date %t (last log term=%d and index=%d)", validVotedFor, upToDate, rf.logAt(lastLogIndex).Term, lastLogIndex))
		if validVotedFor && upToDate {
			rf.role = Candidate // not so sure, can also be Follower
			rf.votedFor = args.CandidateId
			atomic.StoreInt32(&rf.timeoutTicks, randomElectionTick())
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	} // end if
	actionStr := ""
	for _, action := range actions {
		actionStr += "\n\t" + action
	}
	rf.log("Receive from Server %d with request %s. Before State: %s, Reply with %s. Actions: %s ", args.CandidateId, args.String(), beforeState, reply.String(), actionStr)
	rf.persistWithLock()
}

func (rf *Raft) onTimeout() {
	rf.mu.Lock()

	baseIndex := rf.logs[0].Msg.CommandIndex
	if rf.role == Follower {
		rf.role = Candidate
		rf.votedFor = NA
		rf.timeoutTicks = instantTick
		rf.log("Expire from follower to candidate. ")
		rf.mu.Unlock()
		// we depend on next tick to execute onTimeout() as candidate.
		// we could call onTimeout() recursively here, but it would create a nested invocation, not easy to manage.
		// This design will cause an extra tick to wait for the election timeout.
	} else if rf.role == Leader {
		rf.log("Heartbeat Timeout as leader with state %s", rf.String())
		appendEntriesRequests := map[int]*AppendEntryArgs{}
		installSnapshotRequests := map[int]*InstallSnapshotArgs{}
		// prepare requests in a batch with lock
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			} else if rf.nextIndex[i] <= baseIndex {
				// Install Entry
				snapshotData, lastIncludedIndex, lastIncludedTerm := rf.ReadSnapshot()
				if lastIncludedIndex != rf.logs[0].Msg.CommandIndex {
					panic("Should equal")
				}
				installSnapshotRequests[i] = &InstallSnapshotArgs{
					Term:     rf.currentTerm,
					LeaderID: rf.me,
					// Note: these three fields are always modified atomically with rf.mu lock.
					LastIncludedIndex: lastIncludedIndex,
					LastIncludedTerm:  lastIncludedTerm,
					Data:              snapshotData,
				}
			} else {
				appendEntriesRequests[i] = &AppendEntryArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logAt(rf.nextIndex[i] - 1).Term,
					Entries:      []RaftLog{},
					LeaderCommit: rf.commitIdx,
				}
				if firstEmptyLogIndex := rf.logLen(); rf.nextIndex[i] < firstEmptyLogIndex {
					appendEntriesRequests[i].Entries = rf.logs[rf.nextIndex[i]-baseIndex:]
				}

			}
		}
		beforeRole := rf.role
		rf.persistWithLock()
		atomic.StoreInt32(&rf.timeoutTicks, heartbeatTicks)

		rf.mu.Unlock()
		for i, args := range appendEntriesRequests {
			go func(serverIndex int, args *AppendEntryArgs, beforeRole Role) {
				reply := &AppendEntryReply{}
				rf.log("Send to server %d with request %s", serverIndex, args.String())
				if ok := rf.peers[serverIndex].Call("Raft.AppendEntries", args, reply); !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				baseIndex := rf.logs[0].Msg.CommandIndex // baseIndex may change on RPC reply
				var actions []string
				if beforeRole != rf.role || args.Term != rf.currentTerm {
					actions = append(actions, "Ignore msg with inconsistent state between RPC")
				} else if reply.Term < rf.currentTerm {
					actions = append(actions, "Ignore msg with lower term")
				} else if reply.Term > rf.currentTerm {
					stepDownAction := rf.stepDownWithLock(reply.Term)
					actions = append(actions, stepDownAction)
				} else if reply.Success { // reply.Term = rf.currentTerm for the following two cases
					if rf.matchIndex[serverIndex] < args.PrevLogIndex+len(args.Entries) {
						rf.matchIndex[serverIndex] = args.PrevLogIndex + len(args.Entries)
					} else {
						rf.log(fmt.Sprintf("Receive the decreasing match index from server %d", serverIndex))
					}
					rf.nextIndex[serverIndex] = rf.matchIndex[serverIndex] + 1
					actions = append(actions, fmt.Sprintf("Update matchIndex[%d] to %d", serverIndex, rf.matchIndex[serverIndex]))
					actions = append(actions, fmt.Sprintf("Update nextIndex[%d] to %d", serverIndex, rf.nextIndex[serverIndex]))

					matchIndexCpy := make([]int, len(rf.matchIndex))
					copy(matchIndexCpy, rf.matchIndex)
					sort.Slice(matchIndexCpy, func(i, j int) bool {
						return matchIndexCpy[i] < matchIndexCpy[j]
					})
					if tentativeCommitIndex := matchIndexCpy[len(matchIndexCpy)/2]; baseIndex <= tentativeCommitIndex && rf.logAt(tentativeCommitIndex).Term == rf.currentTerm {
						// tentativeCommitIndex may be greater than the baseIndex when the new leader is elected (matchIndex will all be init as 0. )
						actions = append(actions, fmt.Sprintf("Update Commit Index to %d", tentativeCommitIndex))
						rf.CommitWithLock(tentativeCommitIndex)
					}
				} else if reply.ConflictedTerm == NA { // !reply.Success && the follower does not have the log with prevLogIndex
					rf.nextIndex[serverIndex] = reply.ConflictedIndex
					actions = append(actions, fmt.Sprintf("Find that Follower %d does not have the log with prevLogIndex=%d", serverIndex, args.PrevLogIndex))
					actions = append(actions, fmt.Sprintf("Update nextIndex[%d] = %d", serverIndex, rf.nextIndex[serverIndex]))
				} else { // !reply.Success && the follower has the conflicting log with prevLogIndex
					// Based on https://thesquareplanet.com/blog/students-guide-to-raft/ Sec An aside on optimizations
					// Scan logs backward until a log with smaller or equal term
					// Hinge on the fact that the term is monotonically decreasing from backward.
					rf.nextIndex[serverIndex] = baseIndex // in case logs with smaller terms are snapshoted.
					for j := rf.logLen() - 1; j >= baseIndex; j-- {
						if rf.logAt(j).Term == reply.ConflictedTerm {
							rf.nextIndex[serverIndex] = rf.logAt(j).Msg.CommandIndex + 1
							break
						} else if rf.logAt(j).Term < reply.ConflictedTerm {
							rf.nextIndex[serverIndex] = reply.ConflictedIndex
							break
						} else {
							// do nothing to continue backwards scanning
						}
					} // end for
					actions = append(actions, fmt.Sprintf("Find that Follower %d has the conflicting log with prevLogIndex=%d. (Leader Term: %d, Follower Term: %d)", serverIndex, args.PrevLogIndex, args.PrevLogTerm, reply.ConflictedTerm))
					actions = append(actions, fmt.Sprintf("Update nextIndex[%d] = %d", serverIndex, rf.nextIndex[serverIndex]))
				} // end if

				actionStr := ""
				for _, action := range actions {
					actionStr += "\n\t" + action
				}
				rf.log("Receive from server %d with reply %s. Action: %s", serverIndex, reply.String(), actionStr)

			}(i, args, beforeRole)
		}

		for i, args := range installSnapshotRequests {
			go func(serverIndex int, args *InstallSnapshotArgs, beforeRole Role) {
				reply := &InstallSnapshotReply{}
				rf.log("Send to server %d with snapshot request %s", serverIndex, args.String())
				if ok := rf.peers[serverIndex].Call("Raft.InstallSnapshot", args, reply); !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				var actions []string
				if beforeRole != rf.role || args.Term != rf.currentTerm {
					actions = append(actions, "Ignore msg with inconsistent state between RPC")
				} else if reply.Term < rf.currentTerm {
					actions = append(actions, "Ignore msg with lower term")
				} else if reply.Term > rf.currentTerm {
					stepDownAction := rf.stepDownWithLock(reply.Term)
					actions = append(actions, stepDownAction)
				} else if rf.matchIndex[serverIndex] < args.LastIncludedIndex {
					rf.matchIndex[serverIndex] = args.LastIncludedIndex
					rf.nextIndex[serverIndex] = rf.matchIndex[serverIndex] + 1
					actions = append(actions, fmt.Sprintf("Update matchIndex[%d] = %d", serverIndex, rf.matchIndex[serverIndex]))
					actions = append(actions, fmt.Sprintf("Update nextIndex[%d] = %d", serverIndex, rf.nextIndex[serverIndex]))
				} else {
					rf.log("Detect a decreasing match index from Server %d. May be due to the staled reply", serverIndex)
				}
			}(i, args, beforeRole)
		}
	} else { // Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.log("Election Timeout as Candidate with state %s. Increment the term to %d", rf.String(), rf.currentTerm)
		requests := map[int]*RequestVoteArgs{}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			lastIndex := rf.logLen() - 1
			requests[i] = &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.votedFor,
				LastLogIndex: lastIndex,
				LastLogTerm:  rf.logAt(lastIndex).Term,
			}
		}
		atomic.StoreInt32(&rf.timeoutTicks, randomElectionTick())
		beforeRole := rf.role
		rf.persistWithLock()

		rf.mu.Unlock()
		votes := 1 // self-votes
		for i, args := range requests {
			go func(serverIndex int, args *RequestVoteArgs, beforeRole Role) {
				reply := &RequestVoteReply{}
				rf.log("Send to server %d with request %s", serverIndex, args.String())
				if ok := rf.peers[serverIndex].Call("Raft.RequestVote", args, reply); !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()

				var actions []string
				if beforeRole != rf.role || args.Term != rf.currentTerm {
					actions = append(actions, "Ignore msg with inconsistent state between RPC")
				} else if reply.Term < rf.currentTerm {
					actions = append(actions, "Ignore msg with lower term")
				} else if reply.Term > rf.currentTerm {
					stepDownAction := rf.stepDownWithLock(reply.Term)
					actions = append(actions, stepDownAction)
				} else if reply.VoteGranted {
					votes++
					if len(rf.peers)/2 < votes {
						rf.role = Leader
						rf.votedFor = NA
						atomic.StoreInt32(&rf.timeoutTicks, instantTick)
						actions = append(actions, fmt.Sprintf("Elected as the new leader in term %d", rf.currentTerm))
						// Broadcast AppendEntry as the new leader at the next tick
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.logLen()
							if i == rf.me {
								rf.matchIndex[i] = rf.nextIndex[i] - 1
							} else {
								rf.matchIndex[i] = 0
							}
						}
					}
				} else { // !reply.VoteGranted
					// do nothing
				}
				actionStr := ""
				for _, action := range actions {
					actionStr += "\n\t" + action
				}
				rf.log("Receive from server %d with reply %s. Actions: %s", serverIndex, reply.String(), actionStr)
			}(i, args, beforeRole)
		} // end for

	} // end if role
}

func (rf *Raft) stepDownWithLock(newTerm int) string {
	prevRole := rf.role
	prevTerm := rf.currentTerm
	prevVoted := rf.votedFor

	rf.role = Follower
	rf.currentTerm = newTerm
	rf.votedFor = NA
	atomic.StoreInt32(&rf.timeoutTicks, instantTick)
	return fmt.Sprintf("Step down from (Role: %s, Term: %d, VotedFor: %d) to (Role: %s, Term: %d). Set re-election timeout.", prevRole.String(), prevTerm, prevVoted, rf.role, rf.currentTerm)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	firstEmptyLogIndex := rf.logLen()
	term := rf.currentTerm
	isLeader := rf.role == Leader
	if isLeader {
		rf.matchIndex[rf.me] = firstEmptyLogIndex
		rf.nextIndex[rf.me] = firstEmptyLogIndex + 1
		rf.logs = append(rf.logs, RaftLog{term,
			ApplyMsg{
				CommandValid: true,
				Command:      command,
				Term:         term,
				CommandIndex: firstEmptyLogIndex}})
		rf.log("!!!!Accept cmd %v at index %d!!!!", command, firstEmptyLogIndex)
	}
	return firstEmptyLogIndex, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) log(format string, a ...interface{}) {
	if !rf.killed() {
		var args []interface{}
		args = append(args, rf.me) // rf.me is constant, safe for concurrent read
		args = append(args, a...)
		DPrintf("(Raft %d) "+format, args...)
	}
}

func randomElectionTick() int32 {
	return rand.Int31n(electionTimeoutMaxTicks-electionTimeoutMinTicks) + electionTimeoutMinTicks
}

func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) ReadSnapshot() (snapshot []byte, lastIncludedIndex, lastIncludedTerm int) {
	// NOTE: read without holding rf.mu
	snapshotData := rf.persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&snapshot) != nil {
		panic("Fail to decode the persisted snapshot...")
	}
	return
}

func (rf *Raft) SaveSnapshot(snapshot []byte, lastIncludedIndex, lastIncludedTerm int) {
	rf.mu.Lock()
	rf.saveSnapshotWithLock(snapshot, lastIncludedIndex, lastIncludedTerm)
	rf.mu.Unlock()
}

func (rf *Raft) saveSnapshotWithLock(snapshot []byte, lastIncludedIndex, lastIncludedTerm int) {

	lastSnapshotIndex := rf.logs[0].Msg.CommandIndex
	if lastIncludedIndex <= lastSnapshotIndex {
		rf.log("Encounter staled snapshot request up to index %d. Current snapshot index %d", lastIncludedIndex, lastSnapshotIndex)
		return
	}
	newRawIndex := lastIncludedIndex - lastSnapshotIndex + 1 // index relative to the log array
	newLogs := []RaftLog{{lastIncludedTerm, ApplyMsg{false, nil, lastIncludedIndex, lastIncludedTerm}}}
	if newRawIndex < len(rf.logs) {
		newLogs = append(newLogs, rf.logs[newRawIndex:]...)
	}

	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)
	e1.Encode(rf.currentTerm)
	e1.Encode(rf.votedFor)
	e1.Encode(newLogs)
	raftState := w1.Bytes()

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	e2.Encode(lastIncludedIndex)
	e2.Encode(lastIncludedTerm)
	e2.Encode(snapshot)
	snapshotState := w2.Bytes()

	// Note: persister will first persist snapshot state and then raft state
	// This is to prevent the missing log entries in case of a crash between.
	// In this scenario, the newly persisted snapshot may cover some preceding log entries in previous persisted raft state.
	// This duplicated prefix logs will be pruned on the recovery.
	rf.persister.SaveStateAndSnapshot(raftState, snapshotState)
	rf.logs = newLogs // update the volatile at last
	rf.log("Save snapshot to raft index %d and term %d from previous index", lastIncludedIndex, lastIncludedTerm)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	persistence := persister.ReadRaftState()
	snapshot := persister.ReadSnapshot()

	if persistence == nil { // it snapshot must be nil
		rf.currentTerm = 0
		rf.votedFor = NA
		// the first empty log assumes to be committed on each server
		rf.logs = []RaftLog{{0, ApplyMsg{false, nil, 0, 0}}}
		rf.log("Initialize from the scratch")
	} else if snapshot == nil {
		// crashed before the first snapshot is saved
		rf.readPersist(persistence)
	} else {
		rf.readPersist(persistence)
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var lastIncludedIndex int
		var lastIncludedTerm int
		var snapshotData []byte
		if d.Decode(&lastIncludedIndex) != nil ||
			d.Decode(&lastIncludedTerm) != nil ||
			d.Decode(&snapshotData) != nil {
			panic("Fail to decode the persisted snapshot info...")
		} else if rf.logs[0].Msg.CommandIndex < lastIncludedIndex {
			rf.log("Overlap raft log entries from %d to %d", rf.logs[0].Msg.CommandIndex+1, lastIncludedIndex)
			rf.saveSnapshotWithLock(snapshotData, lastIncludedIndex, lastIncludedTerm)
		} else if lastIncludedIndex < rf.logs[0].Msg.CommandIndex {
			panic(fmt.Sprintf("Missing log entries from snapshot %d to raft index %d (both inclusive)", lastIncludedIndex+1, rf.logs[0].Msg.CommandIndex))
		} else {
			rf.log("Match snapshot lastIncludedIndex and raft logs start index %d ", lastIncludedIndex)
		}
	}

	rf.commitIdx = 0
	rf.lastApplied = 0
	rf.role = Candidate
	rf.timeoutTicks = randomElectionTick()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.commitCond = sync.NewCond(&rf.mu)

	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			for rf.commitIdx == rf.lastApplied {
				rf.commitCond.Wait()
			}

			from := rf.lastApplied + 1
			to := rf.commitIdx
			baseIndex := rf.logs[0].Msg.CommandIndex
			rf.lastApplied = max(rf.lastApplied, baseIndex-1)
			for rf.lastApplied < rf.commitIdx {
				rf.applyCh <- rf.logAt(rf.lastApplied + 1).Msg
				rf.lastApplied++
			}
			rf.log("Commits logs from %d to %d", from, to)
			rf.mu.Unlock()
		}
	}()

	go func() {
		for !rf.killed() {
			if atomic.AddInt32(&rf.timeoutTicks, -1) == 0 {
				rf.onTimeout() // either can be election or heartbeat timeout
				if atomic.LoadInt32(&rf.timeoutTicks) == 0 {
					panic("Must set a new timeout event after the execution of a timeout")
				}
			}
			time.Sleep(tickDurationMs * time.Millisecond)
		}
	}()

	return rf
}

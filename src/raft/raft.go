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
	heartbeatTicks          = 15 // send heartbeat in 150ms
)

const (
	Candidate = iota
	Follower
	Leader
)

// abbreviate args.Entries to (Term:term1)[firstIndex:lastIndex], (Term:term2)[firstIndex:lastIndex]
// nil if empty
func abbreviateLogs(logs []RaftLog) string {
	var logAbbr string
	if len(logs) == 0 {
		logAbbr = "nil"
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
	logs        []RaftLog

	// volatile on leaders
	nextIndex  []int
	matchIndex []int

	// volatile
	commitMu   sync.Mutex // Lock to coordinate the commitIndex
	commitCond *sync.Cond // Condition variable to signal when commitIdx is updated.
	// Any access to the following states must be protected by commitMu and occur in a separate gorountine
	commitIdx   int
	lastApplied int
	applyCh     chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
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
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.role == Leader
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
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		panic("Fail to decode the persisted states...")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.log("Initialize from persistence (currentTerm: %d, votedFor: %d, logs: %s", currentTerm, votedFor, abbreviateLogs(logs))
	}
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

func (rf *Raft) Commit(commitIndex int) {
	rf.commitMu.Lock()
	defer rf.commitMu.Unlock()
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

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

		remainingIndex := args.PrevLogIndex + len(args.Entries) + 1
		if len(rf.logs) <= args.PrevLogIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictedIndex = len(rf.logs)
			reply.ConflictedTerm = NA

			actions = append(actions, fmt.Sprintf("Fail to find the log at PrevlogIndex=%d", args.PrevLogIndex))

		} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm

			reply.ConflictedIndex = 0
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if rf.logs[i].Term < rf.logs[args.PrevLogIndex].Term {
					reply.ConflictedIndex = i + 1
					break
				}
			}
			reply.ConflictedTerm = rf.logs[args.PrevLogIndex].Term
			actions = append(actions, fmt.Sprintf("Find the inconsistent log at PrevlogIndex=%d (leader term: %d, follower term: %d)", args.PrevLogIndex, args.PrevLogTerm, rf.logs[args.PrevLogIndex].Term))

			// delete the conflicted entries and all that follow it
			rf.logs = rf.logs[0 : args.PrevLogIndex-1]

		} else if len(rf.logs) <= remainingIndex { // matched index for the following two cases
			if len(args.Entries) > 0 {
				rf.commitMu.Lock() // protect the rf.logs
				rf.logs = append(rf.logs[0:args.PrevLogIndex+1], args.Entries...)
				defer rf.commitMu.Unlock()
				actions = append(actions, fmt.Sprintf("Append logs from index %d to %d", args.PrevLogIndex+1, remainingIndex-1))
			} else {
				actions = append(actions, "Reply the heartbeat for the empty appended entries")
			}

			reply.Success = true
			reply.Term = rf.currentTerm
			reply.ConflictedIndex = NA
			reply.ConflictedTerm = NA
			commitIndex := min(args.LeaderCommit, remainingIndex-1)
			go rf.Commit(commitIndex)
		} else {
			// Only replace the entry with args.Entries. Keep the remaining.
			remaining := rf.logs[remainingIndex:]
			rf.commitMu.Lock() // protect the rf.logs
			rf.logs = append(rf.logs[0:args.PrevLogIndex+1], args.Entries...)
			rf.logs = append(rf.logs, remaining...)
			defer rf.commitMu.Unlock()
			actions = append(actions, fmt.Sprintf("Replace logs from index %d to %d", args.PrevLogIndex+1, remainingIndex-1))

			reply.Success = true
			reply.Term = rf.currentTerm
			reply.ConflictedIndex = NA
			reply.ConflictedTerm = NA

			commitIndex := min(args.LeaderCommit, remainingIndex-1)
			go rf.Commit(commitIndex)
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
	defer rf.mu.Unlock()
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
		lastLogIndex := len(rf.logs) - 1
		if rf.logs[len(rf.logs)-1].Term < args.LastLogTerm {
			upToDate = true
		} else if rf.logs[lastLogIndex].Term == args.LastLogTerm {
			upToDate = lastLogIndex <= args.LastLogIndex
		} else {
			upToDate = false
		}
		actions = append(actions, fmt.Sprintf("Check votedFor %t and up-to-date %t (last log term=%d and index=%d)", validVotedFor, upToDate, rf.logs[lastLogIndex].Term, lastLogIndex))
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
		requests := map[int]*AppendEntryArgs{}
		// prepare requests in a batch with lock
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.commitMu.Lock() // protect rf.commitIdx
			requests[i] = &AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
				Entries:      []RaftLog{},
				LeaderCommit: rf.commitIdx,
			}
			rf.commitMu.Unlock()
			if rf.nextIndex[i] < len(rf.logs) {
				requests[i].Entries = rf.logs[rf.nextIndex[i]:]
			}
		}
		beforeRole := rf.role
		rf.persistWithLock()
		atomic.StoreInt32(&rf.timeoutTicks, heartbeatTicks)
		rf.mu.Unlock()

		for i, args := range requests {
			go func(serverIndex int, args *AppendEntryArgs, beforeRole Role) {
				reply := &AppendEntryReply{}
				rf.log("Send to server %d with request %s", serverIndex, args.String())
				if ok := rf.peers[serverIndex].Call("Raft.AppendEntries", args, reply); !ok {
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
					// panic("  Encounter an AppendEntry Reply with higher term. ")
				} else if reply.Success { // reply.Term = rf.currentTerm for the following two cases
					rf.matchIndex[serverIndex] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[serverIndex] = rf.matchIndex[serverIndex] + 1
					actions = append(actions, fmt.Sprintf("Update matchIndex[%d] to %d", serverIndex, rf.matchIndex[serverIndex]))
					actions = append(actions, fmt.Sprintf("Update nextIndex[%d] to %d", serverIndex, rf.nextIndex[serverIndex]))

					matchIndexCpy := make([]int, len(rf.matchIndex))
					copy(matchIndexCpy, rf.matchIndex)
					sort.Slice(matchIndexCpy, func(i, j int) bool {
						return matchIndexCpy[i] < matchIndexCpy[j]
					})
					if tentativeCommitIndex := matchIndexCpy[len(matchIndexCpy)/2]; rf.logs[tentativeCommitIndex].Term == rf.currentTerm {
						actions = append(actions, fmt.Sprintf("Update Commit Index to %d", tentativeCommitIndex))
						go rf.Commit(tentativeCommitIndex)
					}
				} else if reply.ConflictedTerm == NA { // !reply.Success && the follower does not have the log with prevLogIndex
					rf.nextIndex[serverIndex] = reply.ConflictedIndex
					actions = append(actions, fmt.Sprintf("Find that Follower %d does not have the log with prevLogIndex=%d", serverIndex, args.PrevLogIndex))
					actions = append(actions, fmt.Sprintf("Update nextIndex[%d] = %d", serverIndex, rf.nextIndex[serverIndex]))
				} else { // !reply.Success && the follower has the conflicting log with prevLogIndex
					// Based on https://thesquareplanet.com/blog/students-guide-to-raft/ Sec An aside on optimizations
					// Scan logs backward until a log with smaller or equal term
					// Hinge on the fact that the term is monotonically decreasing from backward.
					for j := len(rf.logs) - 1; j >= 0; j-- {
						if rf.logs[j].Term == reply.ConflictedTerm {
							rf.nextIndex[serverIndex] = rf.logs[j].Msg.CommandIndex + 1
							break
						} else if rf.logs[j].Term < reply.ConflictedTerm {
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
	} else { // Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.log("Election Timeout as Candidate with state %s. Increment the term to %d", rf.String(), rf.currentTerm)
		requests := map[int]*RequestVoteArgs{}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			lastIndex := len(rf.logs) - 1
			requests[i] = &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.votedFor,
				LastLogIndex: lastIndex,
				LastLogTerm:  rf.logs[lastIndex].Term,
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
							rf.nextIndex[i] = len(rf.logs)
							if i == rf.me {
								rf.matchIndex[i] = len(rf.logs) - 1
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
	defer rf.mu.Unlock()

	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := rf.role == Leader
	if isLeader {
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.commitMu.Lock()
		rf.logs = append(rf.logs, RaftLog{term,
			ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: index}})
		rf.log("!!!!Accept cmd %v at index %d!!!!", command, index)
		rf.commitMu.Unlock()
	}
	return index, term, isLeader
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
		DPrintf("(Server %d) "+format, args...)
	}
}

func randomElectionTick() int32 {
	return rand.Int31n(electionTimeoutMaxTicks-electionTimeoutMinTicks) + electionTimeoutMinTicks
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
	if persistence == nil {
		rf.currentTerm = 0
		rf.votedFor = NA
		// the first empty log assumes to be committed on each server
		rf.logs = []RaftLog{{0, ApplyMsg{}}}
		rf.log("Initialize from the scratch")
	} else {
		rf.readPersist(persister.ReadRaftState())
	}

	rf.commitIdx = 0
	rf.lastApplied = 0
	rf.role = Candidate
	rf.timeoutTicks = randomElectionTick()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.commitCond = sync.NewCond(&rf.commitMu)

	go func() {
		for {
			rf.commitMu.Lock()
			for rf.commitIdx == rf.lastApplied {
				rf.commitCond.Wait()
			}

			from := rf.lastApplied + 1
			to := rf.commitIdx
			for rf.lastApplied < rf.commitIdx {
				rf.applyCh <- rf.logs[rf.lastApplied+1].Msg
				rf.lastApplied++
			}
			rf.log("Commits logs from %d to %d", from, to)
			rf.commitMu.Unlock()
		}
	}()

	go func() {
		for {
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

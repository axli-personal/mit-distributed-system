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
	//	"bytes"

	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	applyChannel   chan ApplyMsg
	dead           int32 // set by Kill()
	state          int
	currentTerm    int
	votedFor       int
	log            []LogEntry
	commitIndex    int
	appliedIndex   int
	nextIndex      []int
	matchIndex     []int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.me = me
	rf.applyChannel = applyCh

	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.persister = persister
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(stableHeartbeatTimeout())

	go rf.ticker()
	go rf.commiter()

	return rf
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

type RequestVoteArgs struct {
	Term         int // Candidate term.
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int // Peer term.
	VoteGranted bool
}

func (rf *Raft) canVoteFor(args *RequestVoteArgs) bool {
	// Already voted for someone else.
	if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
		return false
	}

	// At least as up-to-date as my logs.
	myLastLogIndex := len(rf.log) - 1
	myLastLogTerm := rf.log[myLastLogIndex].Term

	if args.LastLogTerm != myLastLogTerm {
		return args.LastLogTerm > myLastLogTerm
	}

	return args.LastLogIndex >= myLastLogIndex
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if !rf.canVoteFor(args) {
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term           int // Leader term.
	PrevLogIndex   int
	PrevLogTerm    int
	CommitLogIndex int // Leader commitIndex.
	Entries        []LogEntry
}

type AppendEntriesReply struct {
	Term    int // Follower term.
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		*reply = AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else {
		rf.electionTimer.Reset(randomElectionTimeout())
	}

	if args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		*reply = AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		return
	}

	i := 0
	for i < len(args.Entries) {
		checkLogIndex := args.PrevLogIndex + 1 + i
		if checkLogIndex >= len(rf.log) {
			break
		}
		if args.Entries[i].Term != rf.log[checkLogIndex].Term {
			rf.log = rf.log[:checkLogIndex]
			break
		}
		i++
	}
	rf.log = append(rf.log, args.Entries[i:]...)

	if args.CommitLogIndex > rf.commitIndex {
		if args.CommitLogIndex < len(rf.log) {
			rf.commitIndex = args.CommitLogIndex
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
		rf.commitAll()
	}

	*reply = AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: true,
	}
}

// Ticker accept timer events and perform corresponding actions.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.startElection()
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			rf.broadcastHeartbeat()
			rf.mu.Unlock()
		}
	}
}

// Must hold lock before start election.
func (rf *Raft) startElection() {
	DPrintf("[server %d] [term %d] start election", rf.me, rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me

	voteCount := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Candidate {
				return
			}
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{}

			// Send RequestVote RPC (release lock in this step).
			rf.mu.Unlock()
			ok := rf.peers[peer].Call("Raft.RequestVote", &args, &reply)
			rf.mu.Lock()

			if !ok {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			if rf.state != Candidate || reply.Term < rf.currentTerm {
				return
			}

			if reply.VoteGranted {
				DPrintf("[server %d] [term %d] receive vote from %d\n", rf.me, rf.currentTerm, peer)
				voteCount += 1
			} else {
				DPrintf("[server %d] [term %d] miss vote from %d\n", rf.me, rf.currentTerm, peer)
				return
			}

			if voteCount > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}(i)
	}

	rf.electionTimer.Reset(randomElectionTimeout())
}

// Must hold lock before boradcast heartbeat.
func (rf *Raft) broadcastHeartbeat() {
	if rf.state != Leader {
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader {
				return
			}
			// Send entries if have any.
			args := AppendEntriesArgs{
				Term:           rf.currentTerm,
				PrevLogIndex:   rf.nextIndex[peer] - 1,
				PrevLogTerm:    rf.log[rf.nextIndex[peer]-1].Term,
				CommitLogIndex: rf.commitIndex,
				Entries:        rf.log[rf.nextIndex[peer]:],
			}
			reply := AppendEntriesReply{}

			rf.mu.Unlock()
			ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
			rf.mu.Lock()

			if !ok {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			if rf.state != Leader {
				return
			}

			if reply.Success {
				// The peer definitely has consistent log entries before matchIndex.
				matchIndex := args.PrevLogIndex + len(args.Entries)
				if matchIndex > rf.matchIndex[peer] {
					rf.matchIndex[peer] = matchIndex
					rf.nextIndex[peer] = matchIndex + 1
				}
			} else {
				rf.nextIndex[peer]--
			}
		}(i)
	}

	rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
}

// Must hold lock before become follower.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionTimer.Reset(randomElectionTimeout())
}

// Must hold lock before become leader.
func (rf *Raft) becomeLeader() {
	DPrintf("[server %d] [term %d] become leader\n", rf.me, rf.currentTerm)

	rf.state = Leader
	rf.electionTimer.Stop()

	// Reset nextIndex and matchIndex.
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	rf.broadcastHeartbeat()
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)

	rf.matchIndex[rf.me] = len(rf.log) - 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			args := AppendEntriesArgs{
				Term:           rf.currentTerm,
				PrevLogIndex:   rf.nextIndex[peer] - 1,
				PrevLogTerm:    rf.log[rf.nextIndex[peer]-1].Term,
				CommitLogIndex: rf.commitIndex,
				Entries:        rf.log[rf.nextIndex[peer]:],
			}
			reply := AppendEntriesReply{}

			rf.mu.Unlock()
			rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
			rf.mu.Lock()

			if reply.Success {
				rf.nextIndex[peer] += len(args.Entries)
				rf.matchIndex[peer] = rf.nextIndex[peer] - 1
			}
		}(i)
	}

	return len(rf.log) - 1, rf.currentTerm, true
}

func (rf *Raft) commitAll() {
	for rf.appliedIndex < rf.commitIndex {
		DPrintf("[server %d] [term %d] commit command at %d", rf.me, rf.currentTerm, rf.appliedIndex+1)

		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.appliedIndex + 1,
			Command:      rf.log[rf.appliedIndex+1].Command,
		}
		rf.applyChannel <- msg

		rf.appliedIndex++
	}
}

func (rf *Raft) commiter() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state == Leader {
			var sortedMatchIndex []int

			sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex...)
			sort.Ints(sortedMatchIndex)

			majorityMatchIndex := sortedMatchIndex[len(rf.peers)/2]

			if majorityMatchIndex > rf.commitIndex && rf.log[majorityMatchIndex].Term == rf.currentTerm {
				rf.commitIndex = majorityMatchIndex
			}

			rf.commitAll()
		}

		rf.mu.Unlock()

		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

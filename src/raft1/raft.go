package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Entry struct {
	Term    int
	Command interface{}
}

type State int

const (
	Invalid State = iota
	Follower
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCond      *sync.Cond
	replicateCond  []*sync.Cond

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVote handler

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) makeRequestVoteArgs() *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	return args
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = Follower
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1 {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()

	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.resetElectionTimer()
	tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		fmt.Sprintf("vote for %d", args.CandidateId),
		fmt.Sprintf("term=%d", rf.currentTerm),
	)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries handler

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int
	Xindex  int
	Xlen    int
}

func (rf *Raft) makeAppendEntriesArgs(peer int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
		Entries:      rf.log[rf.nextIndex[peer]:],
		LeaderCommit: rf.commitIndex,
	}
	if len(args.Entries) > 256 {
		args.Entries = args.Entries[:256]
	}
	return args
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = Follower
	}

	if args.PrevLogIndex >= len(rf.log) {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.Xlen = len(rf.log)
		rf.resetElectionTimer()
		return
	}
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.Xterm = rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 1; i-- {
			if rf.log[i - 1].Term != reply.Xterm {
				reply.Xindex = i
				break
			}
		}
		reply.Xlen = len(rf.log)
		rf.resetElectionTimer()
		return
	}

	rf.resetElectionTimer()
	reply.Term, reply.Success = rf.currentTerm, true
	rf.state = Follower
	tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		fmt.Sprintf("heartbeat from %d", args.LeaderId),
		fmt.Sprintf("term=%d", rf.currentTerm),
	)

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term != entry.Term {
				rf.log = rf.log[:index]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	rf.persist()
	rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	rf.applyCond.Signal()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, Entry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.persist()
	index := len(rf.log) - 1
	tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		fmt.Sprintf("start command %d", index),
		fmt.Sprintf("term=%d, command=%v", rf.currentTerm, command),
	)

	rf.mu.Unlock()
	rf.broadcastHeartbeat(false)
	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
		case <-rf.heartbeatTimer.C:
			rf.broadcastHeartbeat(true)
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		return
	}
	rf.currentTerm= rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.persist()
	rf.state = Candidate
	rf.resetElectionTimer()
	tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		"start election",
		fmt.Sprintf("term=%d", rf.currentTerm),
	)

	votesReceived := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int, term int) {
			args := rf.makeRequestVoteArgs()
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok || rf.state != Candidate || term != rf.currentTerm {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				rf.state = Follower
				rf.resetElectionTimer()
				return
			}
			if reply.VoteGranted {
				votesReceived++
				if votesReceived > len(rf.peers)/2 {
					rf.state = Leader
					tester.Annotate(fmt.Sprintf("Server %d", rf.me),
						"become leader",
						fmt.Sprintf("term=%d", rf.currentTerm),
					)

					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					for i := range rf.peers {
						if i != rf.me {
							rf.replicateCond[i].Signal()
						}
					}
					rf.resetHeartbeatTimer()
				}
			}
		}(peer, rf.currentTerm)
	}
}

func (rf *Raft) broadcastHeartbeat(isheartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	rf.resetHeartbeatTimer()
	tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		"broadcast heartbeat",
		fmt.Sprintf("term=%d", rf.currentTerm),
	)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isheartbeat {
			go rf.heartbeatOnce(peer)
		} else {
			rf.replicateCond[peer].Signal()
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- msg
			tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				fmt.Sprintf("apply command %d", rf.lastApplied),
				fmt.Sprintf("term=%d, command=%v", rf.log[rf.lastApplied].Term, rf.log[rf.lastApplied].Command),
			)
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) replicator(peer int) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.state != Leader || rf.nextIndex[peer] >= len(rf.log) {
			rf.replicateCond[peer].Wait()
		}
		rf.mu.Unlock()
		rf.heartbeatOnce(peer)
	}
}

func (rf *Raft) heartbeatOnce(peer int) {
	args := rf.makeAppendEntriesArgs(peer)
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peer, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = Follower
		rf.resetElectionTimer()
		return
	}

	if reply.Success {
		rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		rf.advanceCommitIndex()
		rf.resetHeartbeatTimer()
		return
	}

	if reply.Xlen < args.PrevLogIndex + 1 {
		rf.nextIndex[peer] = reply.Xlen
		rf.resetHeartbeatTimer()
		return
	}
	
	for i := args.PrevLogIndex; i >= 1; i-- {
		if rf.log[i - 1].Term == reply.Xterm {
			rf.nextIndex[peer] = i
			rf.resetHeartbeatTimer()
			return
		}
		if rf.log[i - 1].Term < reply.Xterm {
			break
		}
	}

	rf.nextIndex[peer] = reply.Xindex
	rf.resetHeartbeatTimer()
}

func (rf *Raft) advanceCommitIndex() {
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			break
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister,
	applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	// Your initialization code here (3A, 3B, 3C).
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,

		state:          Follower,
		electionTimer:  time.NewTimer(electionTimeout()),
		heartbeatTimer: time.NewTimer(heartbeatTimeout()),
		replicateCond:  make([]*sync.Cond, len(peers)),

		currentTerm: 0,
		votedFor:    -1,
		log:         []Entry{{Term: 0, Command: nil}},

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)
	go rf.applier()
	go rf.ticker()

	for i := range rf.peers {
		rf.nextIndex[i], rf.matchIndex[i] = len(rf.log), 0
		if i != rf.me {
			rf.replicateCond[i] = sync.NewCond(&rf.mu)
			go rf.replicator(i)
		}
	}

	return rf
}

func electionTimeout() time.Duration {
	ms := 300 + rand.Intn(500)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(electionTimeout())
}

func heartbeatTimeout() time.Duration {
	ms := 100
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) resetHeartbeatTimer() {
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(heartbeatTimeout())
}

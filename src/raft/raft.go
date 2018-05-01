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
	"labrpc"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

import "bytes"
import "labgob"

const (
	RPC_TIMEOUT         = time.Millisecond * 100
	HEARTBEATS_INTERVAL = time.Millisecond * 50

	STATES_FOLLOWER  = 0
	STATES_CANDIDATE = 1
	STATES_LEADER    = 2

	DEBUG_TERM = -1
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

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term           int
	Leader         int
	Logs           []LogEntry
	LeaderCommitId int
	PreLogIndex    int
	PreLogTerm     int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state        int
	timeoutStamp int64 // ms
	term         int
	sumServer    int
	voteNumber   int
	leaderNumber int
	done         chan struct{}
	applyCh      chan ApplyMsg
	logs         []LogEntry
	commitId     int
	matchId      []int
	preId        []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == STATES_LEADER
}

func (rf *Raft) GetStates() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) SetStates(term, state int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	select {
	case <-rf.done:
		return false
	default:
	}
	if rf.term != term {
		return false
	}
	if state == STATES_LEADER && rf.state != STATES_CANDIDATE {
		return false
	}
	rf.state = state
	if rf.state == STATES_LEADER {
		for i := 0; i < rf.sumServer; i++ {
			rf.matchId[i] = -1
			rf.preId[i] = len(rf.logs) - 1
		}
		rf.matchId[rf.me] = rf.preId[rf.me]
	}
	rf.persist()
	return true
}

func (rf *Raft) UpdateTimeout() {
	rf.timeoutStamp = time.Now().UnixNano()/1000000 + 150 + int64(rand.Intn(150))
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.voteNumber)
	e.Encode(rf.term)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.logs)
	d.Decode(&rf.voteNumber)
	d.Decode(&rf.term)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Number       int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term   int
	IsVote bool
}

func (rf *Raft) VoteCandidate(term, number int) bool {
	if rf.term != term {
		return false
	}
	if rf.voteNumber < 0 || rf.voteNumber == number {
		rf.UpdateTimeout()
		rf.voteNumber = number
		rf.leaderNumber = number
		return true
	}
	return false
}

func (rf *Raft) UpdateTerm(term, leader int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term >= term {
		return false
	}
	rf.term = term
	rf.voteNumber = leader
	rf.leaderNumber = leader
	rf.state = STATES_FOLLOWER
	rf.persist()
	return true
}

func (rf *Raft) GetLastLog() (lastLogTerm int, lastLogIndex int) {
	lastLogTerm = -1
	lastLogIndex = -1
	if len(rf.logs) > 0 {
		lastLogIndex = len(rf.logs) - 1
		lastLogTerm = rf.logs[lastLogIndex].Term
	}
	return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term > rf.term {
		rf.UpdateTerm(args.Term, args.Number)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.IsVote = false

	lastLogTerm, lastLogIndex := rf.GetLastLog()
	if args.Term < rf.term || args.LastLogTerm < lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		return
	}
	reply.IsVote = rf.VoteCandidate(args.Term, args.Number)
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
	return rf.call(server, "Raft.RequestVote", args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term > rf.term {
		rf.UpdateTerm(args.Term, args.Leader)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.Success = false
	if args.Term < rf.term || rf.state != STATES_FOLLOWER {
		return
	}
	rf.UpdateTimeout()
	rf.leaderNumber = args.Leader
	if rf.term < DEBUG_TERM {
		log.Printf("Server %d get heartbeats from:%d, term:%d, my term:%d. [raft.AppendEntries]", rf.me, args.Leader, args.Term, rf.term)
	}
	if len(rf.logs) < args.PreLogIndex+1 || args.PreLogIndex >= 0 && rf.logs[args.PreLogIndex].Term != args.PreLogTerm {
		return
	}
	if len(args.Logs) > 0 {
		//log.Printf("Server %d get PreLogIndex:%d, PreLogTerm:%d, logs length:%d, commitId:%d. [raft.AppendEntries]", rf.me, args.PreLogIndex, args.PreLogTerm, len(args.Logs), args.LeaderCommitId)
	}
	rf.logs = append(rf.logs[:args.PreLogIndex+1], args.Logs...)
	reply.Success = true
	rf.UpdateCommitId(args.LeaderCommitId)
}

func (rf *Raft) call(server int, method string, args interface{}, reply interface{}) bool {
	timeout := time.Tick(RPC_TIMEOUT)
	done := make(chan bool)
	go func() {
		ok := rf.peers[server].Call(method, args, reply)
		done <- ok
	}()
	select {
	case <-timeout:
		return false
	case res := <-done:
		return res
	}
	return false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.call(server, "Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != STATES_LEADER {
		return index, term, isLeader
	}
	select {
	case <-rf.done:
		return index, term, isLeader
	default:
	}

	isLeader = true
	index = len(rf.logs)
	term = rf.term
	rf.logs = append(rf.logs, LogEntry{Command: command, Term: rf.term})
	//log.Printf("Server %d get command %d: %+v. [raft.Start]", rf.me, index, rf.logs[index])
	rf.matchId[rf.me] = index
	rf.persist()

	return index + 1, term, isLeader
}

func (rf *Raft) UpdateCommitId(commitId int) {
	rf.persist()
	if commitId < rf.commitId {
		return
	}
	if commitId >= len(rf.logs) {
		commitId = len(rf.logs) - 1
	}
	for i := rf.commitId + 1; i <= commitId; i++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i + 1}
		rf.commitId = i
		//log.Printf("Server %d commit command %d:%+v, term:%d. [Raft.UpdateCommitId]", rf.me, i, rf.logs[i], rf.term)
	}
}

func (rf *Raft) GetAppendEntriesRequest(server int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	res := &AppendEntriesArgs{
		Term:           rf.term,
		Leader:         rf.me,
		LeaderCommitId: rf.commitId,
		PreLogIndex:    rf.preId[server],
		PreLogTerm:     -1,
	}
	if res.PreLogIndex < len(rf.logs) {
		if res.PreLogIndex >= 0 {
			res.PreLogTerm = rf.logs[res.PreLogIndex].Term
		}
		for i := res.PreLogIndex + 1; i < len(rf.logs); i++ {
			res.Logs = append(res.Logs, rf.logs[i])
		}
		//res.Logs = rf.logs[res.PreLogIndex+1:]
	}
	return res
}

func (rf *Raft) sendHeartbeats() {
	term := rf.term
	var wg sync.WaitGroup
	for i := 0; i < rf.sumServer; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			args := rf.GetAppendEntriesRequest(server)
			args.Term = term
			reply := &AppendEntriesReply{}
			if t, isLeade := rf.GetState(); t != term || !isLeade {
				return
			}
			if !rf.sendAppendEntries(server, args, reply) {
				return
			}
			if reply.Term > rf.term {
				rf.UpdateTerm(reply.Term, -1)
				return
			}
			index := args.PreLogIndex + len(args.Logs)
			//log.Printf("Server %d send logs to %d, result:%v, args:%+v, index:%d. [raft.sendHeartbeats]", rf.me, server, reply.Success, *args, index)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Success {
				if index > rf.preId[server] {
					rf.preId[server] = index
				}
				if index > rf.matchId[server] {
					rf.matchId[server] = index
				}
			} else {
				index := rf.preId[server] - 100
				if index < 0 {
					index = -1
				}
				rf.preId[server] = index
			}
		}(i)
	}
	wg.Wait()
	//log.Println(rf.matchId)
	matchId := append([]int{}, rf.matchId...)
	sort.Ints(matchId)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == STATES_LEADER {
		commitId := matchId[0]
		commitId2 := matchId[rf.sumServer/2]
		if commitId2 > rf.commitId && commitId2 < len(rf.logs) && rf.logs[commitId2].Term == rf.term {
			commitId = commitId2
		}
		if commitId > rf.commitId {
			//log.Printf("Server %d get matchId:%v, update commitId:%d, term:%d. [Raft.sendHeartbeats]", rf.me, rf.matchId, commitId, rf.term)
		}
		rf.UpdateCommitId(commitId)
	}
}

func (rf *Raft) Heartbeats() {
	timeTick := time.Tick(HEARTBEATS_INTERVAL)
	for {
		select {
		case <-timeTick:
		case <-rf.done:
			return
		}
		if rf.GetStates() != STATES_LEADER {
			return
		}
		go rf.sendHeartbeats()
	}
}

func (rf *Raft) StartElection() {
	term := rf.term + 1
	rf.UpdateTimeout()
	if !rf.UpdateTerm(term, rf.me) {
		return
	}
	if !rf.SetStates(term, STATES_CANDIDATE) {
		return
	}

	voteChan := make(chan bool, rf.sumServer)
	sumVote := 1
	rf.mu.Lock()
	lastLogTerm, lastLogIndex := rf.GetLastLog()
	rf.mu.Unlock()
	for i := 0; i < rf.sumServer; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				Number:       rf.me,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				if reply.Term > rf.term {
					rf.UpdateTerm(reply.Term, -1)
				}
				//log.Printf("Server %d send vote to %d:%v, term:%d. [raft.StartElection]", server, rf.me, reply.IsVote, term)
			}
			voteChan <- reply.IsVote
		}(i)
	}
	for i := 0; i < rf.sumServer-1; i++ {
		if <-voteChan {
			sumVote += 1
		}
	}
	//log.Printf("Server %d/%d get vote:%d, term:%d, now term:%d, state:%d. [raft.StartElection]", rf.me, rf.sumServer, sumVote, term, rf.term, rf.GetStates())
	if rf.term != term || rf.GetStates() != STATES_CANDIDATE {
		return
	}
	if sumVote > rf.sumServer/2 && rf.SetStates(term, STATES_LEADER) {
		//log.Printf("Server %d become leader, term:%d, lastLogTerm:%d, lastLogIndex:%d. [raft.StartElection]", rf.me, rf.term, lastLogTerm, lastLogIndex)
		rf.leaderNumber = rf.me
		go rf.Heartbeats()
	}
}

func (rf *Raft) StartHeartbeats() {
	for {
		nowMs := time.Now().UnixNano() / 1000000
		interval := rf.timeoutStamp - nowMs
		if interval <= 0 {
			if rf.GetStates() != STATES_LEADER {
				if rf.term < DEBUG_TERM {
					log.Printf("Server %d timeout on %d, term:%d. [raft.StartHeartbeats]", rf.me, nowMs%10000, rf.term)
				}
				rf.StartElection()
			}
			interval = 1
		}
		timeTick := time.Tick(time.Duration(interval) * time.Millisecond)
		select {
		case <-timeTick:
			continue
		case <-rf.done:
			return
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	close(rf.done)
}

func (rf *Raft) Init() {
	rf.term = 0
	rf.UpdateTimeout()
	rf.sumServer = len(rf.peers)
	rf.done = make(chan struct{})
	rf.commitId = -1
	rf.matchId = make([]int, rf.sumServer)
	rf.preId = make([]int, rf.sumServer)
	rf.state = STATES_FOLLOWER
	go rf.StartHeartbeats()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.Init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

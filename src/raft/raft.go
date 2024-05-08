package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (index, term, isleader)
// ask a Raft for its current term, and whether it thinks it is leader
//		rf.GetState() (term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"fmt"
	"math/rand"
	"project3/src/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg // Channel for the commit to the state machine

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state   int // 0: follower, 1: candidate, 2: leader
	stopped bool

	// persistent state on all servers
	currentTerm int
	votedFor    int   //seems to be current leader id
	log         []Log // not sure which type it should be

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// heartbeat stuff
	heartbeatTimer int
	heartbeatChan  chan AppendEntriesArgs
	stopCh         chan bool

	// election stuff
	electionTimer int
	chWinElection chan bool
}

type Log struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.

// MAYBE DONE
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (3).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4).
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
	// Your code here (4).
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!

// PROBABLY DONE
type RequestVoteArgs struct {
	// Your data here (3, 4).

	// Figure 2 RequestVote RPC arguments
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!

// PROBABLY DONE
type RequestVoteReply struct {
	// Your data here (3).

	// Figure 2 RequestVote RPC replies
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// arguments

	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit int
}

type AppendEntriesResults struct {
	// results
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3, 4).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("server %v received a vote request\n", rf.me)

	// no vote
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// update rf term if the candidate has higher term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogIndex >= rf.lastApplied) {
		fmt.Printf("server %v granted a vote request\n", rf.me)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.electionTimeGenerator()
		return
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResults) {
	// Your code here (3, 4).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("Follower %d receiving heartbeats; Term: %d\n", rf.me, rf.currentTerm)

	if (args.Term < rf.currentTerm) || (args.PrevLogIndex > len(rf.log)) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
	}

	for index, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + index
		if pos < len(rf.log) {
			if rf.log[pos].Term != entry {
				rf.log = rf.log[:pos] // truncate the log
			}
		}
		rf.log = append(rf.log, Log{0, entry})
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}
	reply.Success = true
	reply.Term = rf.currentTerm

	rf.heartbeatChan <- *args
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, returns *AppendEntriesResults) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, returns)
	//fmt.Printf("Follower %d received heartbeat from Leader %d; Term: %d\n", server, args.LeaderId, args.Term)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (4).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.

	rf.mu.Lock()
	rf.stopped = true
	rf.mu.Unlock()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = 0

	// Your initialization code here (3, 4).
	rf.heartbeatChan = make(chan AppendEntriesArgs)
	rf.chWinElection = make(chan bool, 1)
	rf.stopCh = make(chan bool)
	rf.mu.Unlock()
	rf.electionTimeGenerator()
	rf.heartBeatTimeGenerator()

	go func() {
		rf.startInit()
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// check state conditions
func (rf *Raft) startInit() {
	rf.mu.Lock()
	rf.stopped = false
	rf.mu.Unlock()

	for !rf.stopped {
		switch rf.state {
		case 0:
			// follower
			select {

			// leader failure
			case <-time.After(time.Duration(rf.safeGetElectionTime()) * time.Millisecond):
				// heartbeat timeout case
				fmt.Printf("F%v hasn't received heartbeat. Stepping up to candidate C%v...\n", rf.me, rf.me)
				rf.mu.Lock()
				rf.state = 1
				rf.mu.Unlock()

				// go to elections right away
				rf.elections()

			// recieve a heartbeat
			case heartbeat := <-rf.heartbeatChan:
				fmt.Printf("F%v has received heartbeat. Staying as follower...\n", rf.me)
				rf.electionTimeGenerator()
				if heartbeat.Term > rf.safeGetTerm() {
					rf.mu.Lock()
					rf.currentTerm += 1
					rf.mu.Unlock()
				}
			}
		case 1:
			// candidate
			fmt.Printf("C%v hasn't received heartbeat. Starting initial election C%v...\n", rf.me, rf.me)

			select {
			case <-time.After(time.Duration(rf.safeGetElectionTime()) * time.Millisecond):
				fmt.Printf("C%v election has timed out. Start new election.\n", rf.me)
				// start elections
				go rf.elections()

			case heartbeat := <-rf.heartbeatChan:
				if heartbeat.Term > rf.safeGetTerm() {
					fmt.Printf("C%v has received heartbeat. Stepping down to follower F%v...\n", rf.me, rf.me)
					rf.mu.Lock()
					rf.state = 0
					rf.mu.Unlock()
				}

			case win := <-rf.chWinElection:
				if win {
					rf.mu.Lock()
					rf.state = 2
					rf.mu.Unlock()
					fmt.Printf("Candidate %d won the election and is now the Leader.\n", rf.me)
					// go rf.sendHeartBeats()
					go rf.broadcastHeartbeat()
				}
			}

		case 2:
			// leader
			select {
			case <-time.After((time.Duration(100)) * time.Millisecond):
				rf.mu.Lock()
				if rf.state != 2 {
					fmt.Printf("L%v NOT THE LEADER", rf.me)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				rf.broadcastHeartbeat()
			case heartbeat := <-rf.heartbeatChan:
				if heartbeat.Term > rf.safeGetTerm() {
					fmt.Printf("L%v has received heartbeat. Stepping down to follower F%v...\n", rf.me, rf.me)
					rf.mu.Lock()
					rf.state = 0
					rf.currentTerm = heartbeat.Term
					rf.mu.Unlock()
				}
			}
		}
	}
}

func (rf *Raft) safeGetTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm
}

func (rf *Raft) heartBeatTimeGenerator() {
	rf.mu.Lock()
	rf.heartbeatTimer = rand.Intn(100) + 200
	rf.mu.Unlock()
}

func (rf *Raft) safeGetHeartBeatTime() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.heartbeatTimer
}

func (rf *Raft) electionTimeGenerator() {
	rf.electionTimer = rand.Intn(200) + 300
}

func (rf *Raft) safeGetElectionTime() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.electionTimer
}

func (rf *Raft) elections() {
	rf.mu.Lock()
	fmt.Printf("Node %d starting election for term %d\n", rf.me, rf.currentTerm+1)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	rf.electionTimeGenerator()

	rf.electionTimeGenerator()

	votes := int32(1) // start at 1 because voted for self
	voteChannels := make(chan bool, len(rf.peers)-1)

	for index := range rf.peers {
		if index != rf.me {

			go func(server int) {

				// making sure we don't overflow the log
				var lli int
				var llt int
				if (len(rf.log) - 1) < 0 {
					lli = 0
					llt = 0
				} else {
					lli = len(rf.log) - 1
					llt = rf.log[len(rf.log)-1].Term
				}

				// get votes
				requestArgs := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lli,
					LastLogTerm:  llt,
				}
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(server, &requestArgs, &reply); ok && reply.VoteGranted {
					voteChannels <- true
				} else {
					voteChannels <- false
				}
			}(index)
		}
	}

	// PROTECTED, ALWAYS UPDATING VOTES WHILE STILL SENNDING OUT VOTE REQUESTS
	go func() {
		for vote := range voteChannels {
			if vote {
				atomic.AddInt32(&votes, 1)
			}
		}
	}()

	// if winner then setup new leader, else do nothing -- set a max timeout so it doesn't run forever
	go func() {
		maxTimeOut := time.After(time.Duration(500) * time.Millisecond)
		for {
			select {
			case <-maxTimeOut:
				fmt.Printf("Node %d timed out\n", rf.me)
				return
			default:
				if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
					fmt.Printf("Node %d received %d votes\n", rf.me, votes)
					fmt.Printf("Node %d becomes leader\n", rf.me)
					go func() { rf.chWinElection <- true }() //do this in loop above
					return
				}
			}
		}
	}()
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}
	defer rf.mu.Unlock()

	fmt.Printf("L%v is sending out heartbeats. Staying the leader L%v...\n", rf.me, rf.me)
	var lli int // need to make sure we don't overflow
	var llt int
	if (len(rf.log) - 1) < 0 {
		lli = 0
		llt = 0
	} else {
		lli = len(rf.log) - 1
		llt = rf.log[len(rf.log)-1].Term
	}

	// Send heartbeats to all servers
	entryArgs := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lli,
		PrevLogTerm:  llt,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	for index := range rf.peers {
		if index != rf.me {
			go func(server int) {
				reply := AppendEntriesResults{}
				rf.sendAppendEntries(server, &entryArgs, &reply)
			}(index)
		}
	}
}

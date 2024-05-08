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
	heartbeatChan chan AppendEntriesArgs

	// election stuff
	electionTimer     int
	chElectionResults chan bool
}

type Log struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
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

	// RECEIVING VOTE REQUEST

	//fmt.Printf("server %v received a vote request\n", rf.me)

	// if we are at greater term, do not vote
	if args.Term < rf.safeGetTerm() {
		reply.Term = rf.safeGetTerm()
		reply.VoteGranted = false
		return
	}

	// update rf term if the candidate has higher term
	if args.Term > rf.safeGetTerm() {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.mu.Unlock()
	}

	// vote if conditions are right
	rf.mu.Lock()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogIndex >= rf.lastApplied) {
		//fmt.Printf("server %v granted a vote request\n", rf.me)
		rf.votedFor = args.CandidateId
		rf.electionTimeGenerator()
		rf.mu.Unlock()
		reply.Term = rf.safeGetTerm()
		reply.VoteGranted = true
		return
	} else {
		rf.mu.Unlock()
		reply.Term = rf.safeGetTerm()
		reply.VoteGranted = false
		return
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResults) {
	// Your code here (3, 4).
	//fmt.Printf("Follower %d receiving heartbeats; Term: %d\n", rf.safeGetMe(), rf.safeGetTerm())

	// RECEIVING HEARTBEATS

	// if heartbeat is from old term, do not accept it
	if (args.Term < rf.safeGetTerm()) || (args.PrevLogIndex > len(rf.log)) {
		reply.Term = rf.safeGetTerm()
		reply.Success = false
		return
	}

	// if heartbeat is from later term, update state variables
	if args.Term > rf.safeGetTerm() {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		rf.mu.Unlock()
	}

	// update log
	for index, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + index
		rf.mu.Lock()
		if pos < len(rf.log) {
			if rf.log[pos].Term != entry {
				rf.log = rf.log[:pos] // truncate the log
			}
		}
		rf.log = append(rf.log, Log{0, entry})
		rf.mu.Unlock()
	}

	// update commit index
	rf.mu.Lock()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}
	rf.mu.Unlock()

	// success! reply
	reply.Success = true
	reply.Term = rf.safeGetTerm()

	// Update heartbeat channel to keep track of state correctly
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
	// if killed, then stop the server
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
	// initialize our variables
	rf.heartbeatChan = make(chan AppendEntriesArgs)
	rf.chElectionResults = make(chan bool, 1)
	rf.electionTimeGenerator()
	rf.mu.Unlock()

	// start the raft!
	go func() {
		rf.startInit()
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// check state conditions and transition accordingly
func (rf *Raft) startInit() {

	// at first everyone is going!
	rf.mu.Lock()
	rf.stopped = false
	rf.mu.Unlock()

	// while not deactivated, keep track of state
	for !rf.safeGetStopped() {

		switch rf.safeGetState() {

		case 0:

			// follower
			select {

			// case 1: timer timed out without receiving a heartbeat meaning leader failure so become a candidate
			case <-time.After(time.Duration(rf.safeGetElectionTime()) * time.Millisecond):
				// heartbeat timeout case
				// fmt.Printf("F%v hasn't received heartbeat. Stepping up to candidate C%v...\n", rf.safeGetMe(), rf.safeGetMe())
				rf.mu.Lock()
				rf.state = 1
				rf.mu.Unlock()

				// go to elections right away
				rf.elections()

			// case 2: recieve a heartbeat so remain follower
			case heartbeat := <-rf.heartbeatChan:
				//fmt.Printf("F%v has received heartbeat. Staying as follower...\n", rf.safeGetMe())
				rf.mu.Lock()
				rf.electionTimeGenerator()
				rf.mu.Unlock()

				if heartbeat.Term > rf.safeGetTerm() {
					rf.mu.Lock()
					rf.currentTerm += 1
					rf.mu.Unlock()
				}
			}

		case 1:

			// candidate
			//fmt.Printf("C%v hasn't received heartbeat. Starting initial election C%v...\n", rf.safeGetMe(), rf.safeGetMe())

			select {

			// case 1: election timeout is over without having become leader, so restart election
			case <-time.After(time.Duration(rf.safeGetElectionTime()) * time.Millisecond):
				//fmt.Printf("C%v election has timed out. Start new election.\n", rf.safeGetMe())
				// start elections
				rf.elections()

			// case 2: received a heartbeat from new leader. if leader has greater term, go back to being a follower
			case heartbeat := <-rf.heartbeatChan:
				if heartbeat.Term > rf.safeGetTerm() {
					//fmt.Printf("C%v has received heartbeat. Stepping down to follower F%v...\n", rf.safeGetMe(), rf.safeGetMe())
					rf.mu.Lock()
					rf.state = 0
					rf.mu.Unlock()
				}

			// case 3: won the election! become leader and broadcast heartbeats right away
			case win := <-rf.chElectionResults:
				if win {
					rf.mu.Lock()
					rf.state = 2
					rf.mu.Unlock()
					//fmt.Printf("Candidate %d won the election and is now the Leader.\n", rf.safeGetMe())
					rf.broadcastHeartbeat()
				}
			}

		case 2:

			// leader
			select {

			// case 1: not deactivated, sending heartbeats 10 times per second
			case <-time.After((time.Duration(100)) * time.Millisecond):
				rf.mu.Lock()
				if rf.state != 2 {
					//fmt.Printf("L%v NOT THE LEADER\n", rf.me)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				rf.broadcastHeartbeat()

			// case 2: received a heartbeat, if heartbeat is from later term, then go back to being a follower
			case heartbeat := <-rf.heartbeatChan:
				if heartbeat.Term > rf.safeGetTerm() {
					//fmt.Printf("L%v has received heartbeat. Stepping down to follower F%v...\n", rf.safeGetMe(), rf.safeGetMe())
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
	// ensuring safe calling of raft variable
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm
}

func (rf *Raft) safeGetMe() int {
	// ensuring safe calling of raft variable
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.me
}

func (rf *Raft) safeGetPeers() []*labrpc.ClientEnd {
	// ensuring safe calling of raft variable
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.peers
}

func (rf *Raft) safeGetCommitIndex() int {
	// ensuring safe calling of raft variable
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.commitIndex
}

func (rf *Raft) safeGetState() int {
	// ensuring safe calling of raft variable
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state
}

func (rf *Raft) safeGetStopped() bool {
	// ensuring safe calling of raft variable
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.stopped
}

func (rf *Raft) electionTimeGenerator() {
	// for getting random election timer (not protected her, make sure protect when called)
	rf.electionTimer = rand.Intn(200) + 300
}

func (rf *Raft) safeGetElectionTime() int {
	// ensuring safe calling of raft variable
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.electionTimer
}

func (rf *Raft) elections() {

	// update state variables for elections
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.electionTimeGenerator()
	rf.mu.Unlock()

	//fmt.Printf("Node %d starting election for term %d\n", rf.safeGetMe(), rf.safeGetTerm()+1)

	// variables to keep track of votes
	votes := int32(1) // start at 1 because voted for self
	voteChannels := make(chan bool, len(rf.safeGetPeers())-1)

	// send out vote requests
	for index := range rf.safeGetPeers() {
		if index != rf.safeGetMe() {

			go func(server int) {

				// making sure we don't overflow the log
				var lli int
				var llt int
				rf.mu.Lock()
				if (len(rf.log) - 1) < 0 {
					lli = 0
					llt = 0
				} else {
					lli = len(rf.log) - 1
					llt = rf.log[len(rf.log)-1].Term
				}
				rf.mu.Unlock()

				// get votes
				requestArgs := RequestVoteArgs{
					Term:         rf.safeGetTerm(),
					CandidateId:  rf.safeGetMe(),
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
				//fmt.Printf("Node %d timed out\n", rf.safeGetMe())
				return
			default:
				if atomic.LoadInt32(&votes) > int32(len(rf.safeGetPeers())/2) {
					//fmt.Printf("Node %d received %d votes\n", rf.safeGetMe(), atomic.LoadInt32(&votes))
					//fmt.Printf("Node %d becomes leader\n", rf.safeGetMe())
					go func() { rf.chElectionResults <- true }() //do this in loop above
					return
				}
			}
		}
	}()
}

func (rf *Raft) broadcastHeartbeat() {

	// check indeed leader
	rf.mu.Lock()
	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	//fmt.Printf("L%v is sending out heartbeats. Staying the leader L%v...\n", rf.safeGetMe(), rf.safeGetMe())

	// need to make sure we don't overflow
	var lli int
	var llt int
	rf.mu.Lock()
	if (len(rf.log) - 1) < 0 {
		lli = 0
		llt = 0
	} else {
		lli = len(rf.log) - 1
		llt = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()

	// Send heartbeats to all servers
	entryArgs := AppendEntriesArgs{
		Term:         rf.safeGetTerm(),
		LeaderId:     rf.safeGetMe(),
		PrevLogIndex: lli,
		PrevLogTerm:  llt,
		Entries:      nil,
		LeaderCommit: rf.safeGetCommitIndex(),
	}

	for index := range rf.safeGetPeers() {
		if index != rf.safeGetMe() {
			go func(server int) {
				reply := AppendEntriesResults{}
				rf.sendAppendEntries(server, &entryArgs, &reply)
			}(index)
		}
	}
}

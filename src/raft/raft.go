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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "sort"
import "fmt"

import "bytes"
import "labgob"

// Roles
const (
	Follower = iota
	Candidate
	Leader
)

const leaderCheckDelta int64 = 1e8 // 0.1s
const leaderStepDownDelta int64 = 3e8
const electionTimeoutBase int64 = 2e8
const electionTimeoutRange int64 = 6e8
const heartBeatDelta int64 = 1e8
const applyCheckDelta int64 = 2e7
const maxAppendEntries int = 100

const clientDataEntryType    int = 0
const noopEntryType          int = 1 // not used except the first empty entry

func myDebug(other ...interface{}) {
	/*
	fmt.Print(time.Now().String()[14:25], " raft:")
	fmt.Println(other...) 
	// */
	// fmt.Print("")
}

func snapDebug(other ...interface{}) {
	/*
	fmt.Print(time.Now().String()[14:25], " raft:")
	fmt.Println(other...) 
	// */
	// fmt.Print("")
}

// ApplyMsg :
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
	PersistStateSize int
	Snapshot     []byte
}

type Entry struct {
	Data interface{}
	Term int
	EntryType      int // 0 for client data, 1 for noop, 2 for ...
}

// Raft :
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
	
	// persistent status
	currentTerm int // what term it believes in
	votedFor    int // candidate index it voted to in this term, -1 for never voted
	log         []Entry
	// persist in snapshot
	lastIndexInSnapshot int // init to -1
	lastTermInSnapshot  int // init to -1

	// volatile status
	committedIndex int // 0 initial
	lastApplied    int // 0 initial
	// volatile status of leader, reinitialize when newly elected
	nextIndex      []int
	matchIndex     []int
	
	lastLeaderTS   int64 // last time when leader called
	role           int // follower, candidate, leader
	leader         int // the leader index that this one believes in, init with -1
	terminate      bool

	appendNoticeChan chan interface {}

	// applyChan      chan ApplyMsg
	newSnapshotInstalled  bool
}

func (rf *Raft) logLength() int {
	return rf.lastIndexInSnapshot + 1 + len(rf.log)
}

func (rf *Raft) idxToTailIdx(idx int) int {
	return idx - rf.lastIndexInSnapshot - 1
}

func (rf *Raft) getEntry(index int) *Entry {
	return &rf.log[rf.idxToTailIdx(index)]
}
func (rf *Raft) getTerm(index int) int {
	tailIdx := rf.idxToTailIdx(index)
	if tailIdx == -1 {
		return rf.lastTermInSnapshot
	} else if tailIdx >= 0 {
		return rf.log[tailIdx].Term
	} else {
		panic(fmt.Sprint(rf.me, " Accessing term in snapshot, idx=", index, ",lastIdx=", rf.lastIndexInSnapshot))
	}
}

// the index is not included
func (rf *Raft) discardLogFrom(index int) {
	if index <= rf.lastIndexInSnapshot {
		panic("discarding logs in snapshot!")
	}
	rf.log = rf.log[:rf.idxToTailIdx(index)]
}

func (rf *Raft) appendEntry(entry *Entry) {
	rf.log = append(rf.log, *entry)
}

func (rf *Raft) getLogSlice(fromIdx int, toIdx int) []Entry {
	if toIdx == -1 {
		return rf.log[rf.idxToTailIdx(fromIdx) : ]
	} else {
		return rf.log[rf.idxToTailIdx(fromIdx) : rf.idxToTailIdx(toIdx)]
	}
}

// GetState :
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	term, role := rf.currentTerm, rf.role == Leader
	rf.mu.Unlock()
	return term, role
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// Warn: must call this inside lock!
func (rf *Raft) persist() {
	// Your code here (2C).
	wbuf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(wbuf)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	data := wbuf.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistWithSMSnapshot(stateMachineData []byte, lastIndex int, lastTerm int) {
	wbuf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(wbuf)
	encoder.Encode(stateMachineData)
	encoder.Encode(lastIndex)
	encoder.Encode(lastTerm)
	snapshotData := wbuf.Bytes()
	
	wbuf = new(bytes.Buffer)
	encoder = labgob.NewEncoder(wbuf)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	rfData := wbuf.Bytes()

	rf.persister.SaveStateAndSnapshot(rfData, snapshotData)
}

func (rf *Raft) persistWithEntireSnapshot(snapshot []byte) {
	wbuf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(wbuf)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	rfData := wbuf.Bytes()

	rf.persister.SaveStateAndSnapshot(rfData, snapshot)

}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(rfData []byte, snapshot []byte) {
	if rfData != nil && len(rfData) >= 1 { // bootstrap without any state?
		// return
		rbuf := bytes.NewBuffer(rfData)
		decoder := labgob.NewDecoder(rbuf)
		var currentTerm int
		var votedFor int
		var log []Entry
		if decoder.Decode(&currentTerm) != nil ||
			 decoder.Decode(&votedFor) != nil || 
			 decoder.Decode(&log) != nil {
			// error...
		} else {
			rf.currentTerm = currentTerm
			rf.votedFor = votedFor
			rf.log = log
		}
	}
	if snapshot != nil && len(snapshot) >= 1 {
		rbuf := bytes.NewBuffer(snapshot)
		decoder := labgob.NewDecoder(rbuf)
		var smSnapshot []byte
		var lastIndex int
		var lastTerm int
		if decoder.Decode(&smSnapshot) != nil ||
			 decoder.Decode(&lastIndex) != nil || 
			 decoder.Decode(&lastTerm) != nil {
			// error...
		} else {
			rf.lastIndexInSnapshot = lastIndex
			rf.lastTermInSnapshot  = lastTerm
			rf.newSnapshotInstalled = true
			rf.lastApplied = lastIndex
			snapDebug(rf.me, "newinstalled=true, loading snapshot from start. lastIdx,lastterm=", lastIndex, lastTerm)
		}
	}
	// Your code here (2C).
}

func findCommitIndex(matchIndex []int) int {
	// the leader self's matchIndex is always 0
	locallist := make([]int, len(matchIndex))
	for i:= 0; i < len(matchIndex); i++ {
		locallist[i] = matchIndex[i]
	}
	// sort(locallist)
	sort.Ints(locallist)
	return locallist[(len(matchIndex)+1)/2]
	// 2: 1
	// 3: 2
	// 4: 2
	// 5: 3
	// 6: 3
}




// RequestVoteArgs :
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term            int
	CandidateId     int
	LastLogIndex    int
	LastLogTerm     int
}

// RequestVoteReply :
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term            int
	VoteGranted     bool
}


// RequestVote :
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		myDebug(rf.me, "<-", args.CandidateId, " :reject voting due to local new term ", 
						rf.currentTerm, " and old remote ", args.Term)
		// I'm newer
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.currentTerm < args.Term {
		// grant vote
		myDebug(rf.me, "<-", args.CandidateId, " :recv newer term reqVote, move term  ", 
						rf.currentTerm, " to ", args.Term)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.leader = -1
		rf.votedFor = -1
		// do not start election 
		// rf.lastLeaderTS = time.Now().UnixNano()
		
		localLastIndex := rf.logLength()-1
		localLastTerm := rf.getTerm(localLastIndex)
		if localLastTerm > args.LastLogTerm || 
				(localLastTerm == args.LastLogTerm && localLastIndex > args.LastLogIndex) {
			// reject
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			myDebug(rf.me, "<-", args.CandidateId, " :reject voting due to local logs (", 
							localLastTerm, localLastIndex, ")>(", args.LastLogTerm, args.LastLogIndex, ")")
		} else {
			myDebug(rf.me, "<-", args.CandidateId, " :grant voting due to remote logs (", 
							args.LastLogTerm, args.LastLogIndex, ")>=(", localLastTerm, localLastIndex, ")")
			// accept
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		}
		rf.persist()
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.role == Follower {
		myDebug(rf.me, "<-", args.CandidateId, " :recv reqVote, t=", args.Term)
		// grant vote
		localLastIndex := rf.logLength()-1
		localLastTerm := rf.getTerm(localLastIndex)
		if localLastTerm > args.LastLogTerm || 
				(localLastTerm == args.LastLogTerm && localLastIndex > args.LastLogIndex) {
			// reject
			myDebug(rf.me, "<-", args.CandidateId, " :reject voting due to local logs (", 
							localLastTerm, localLastIndex, ")>(", args.LastLogTerm, args.LastLogIndex, ")")
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		} else {
			// accept
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.Term = rf.currentTerm
			rf.lastLeaderTS = time.Now().UnixNano()
			reply.VoteGranted = true
			myDebug(rf.me, "<-", args.CandidateId, " :grant voting due to remote logs (", 
							args.LastLogTerm, args.LastLogIndex, ")>=(", localLastTerm, localLastIndex, ")")
		}
	} else {
		myDebug(rf.me, "<-", args.CandidateId, " reject voting due to voted or not follower")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int // what does this do?
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int // leader's commit Index
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// this should set local timestamp
	
	// when to update:
	// rf.term = args.term 
	// rf.term = args.term and rf.role = candidate:
	//    rf.role = follower, rf.leader = args.id
	// rf.term < args.term
	//    rf.role = follower, rf.leader = args.id, rf.term = args.term

	// when not to update:
	// rf.term = args.term, rf.role = follower, rf.leader = args.id(must if not -1)

	rf.mu.Lock()

	if rf.currentTerm > args.Term {
		myDebug(rf.me, "<-", args.LeaderId, ": recved heart beat(", args.Term, ") is stale(", 
						rf.currentTerm, ")")
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.lastLeaderTS = time.Now().UnixNano()

	reply.Term = args.Term

	if rf.currentTerm == args.Term && rf.role == Follower && rf.leader != -1 {
		// nothing to modify here
		if (rf.leader != args.LeaderId) {
			panic("Bug detected: different leader for same term")
		}
	} else if rf.currentTerm < args.Term {
		// catch up the new term
		rf.votedFor = -1
		myDebug(rf.me, "<-", args.LeaderId, " :append, move term from ", 
						rf.currentTerm, " to ", args.Term)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.leader = args.LeaderId
		rf.lastLeaderTS = time.Now().UnixNano()
		rf.persist()
	} else {
		// maybe need to learn the current term's leader, give up election
		rf.lastLeaderTS = time.Now().UnixNano()
		rf.role = Follower
		rf.leader = args.LeaderId
		myDebug(rf.me, "<-", args.LeaderId, " :append term checked.", 
						" may give up election in the same term=", args.Term)
	}

	if args.PrevLogIndex >= rf.logLength() {
		// i have shorter Log
		myDebug(rf.me, "<-", args.LeaderId, " :reject append due to missing logs, ", 
						"(pI, localLen)=", args.PrevLogIndex, rf.logLength())
		reply.Success = false
	} else if rf.lastIndexInSnapshot > args.PrevLogIndex {
		// TODO: Part3B: if prevLogIndex < lastIndexSnapshot
		snapDebug(rf.me, "<-", args.LeaderId, " :reject append due to previdx less than lastidxinsnapshot,",
		" (pI, pT, lastIdxInSnapshot)=", args.PrevLogIndex, args.PrevLogTerm, rf.lastIndexInSnapshot)
	} else if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm {
		myDebug(rf.me, "<-", args.LeaderId, " :reject append due to diff prev term,",
						" (pI, pT, localTerm)=", args.PrevLogIndex, args.PrevLogTerm, rf.getTerm(args.PrevLogIndex))
		reply.Success = false
	} else {
		if len(args.Entries) != 0 {
			myDebug(rf.me, "<-", args.LeaderId, " :accept append some entries, len,lastTerm=", len(args.Entries), args.Entries[len(args.Entries)-1].Term)
		}
		// now update local entries
		nextEntryToPut := 0
		nextIndexToPut := args.PrevLogIndex + 1
		logModified := false
		// if the nextIndexToPut is already in rf.Log, check the term
		for ; nextIndexToPut < rf.logLength() && nextEntryToPut < len(args.Entries); nextIndexToPut++ {
			if rf.getTerm(nextIndexToPut) != args.Entries[nextEntryToPut].Term {
				rf.discardLogFrom(nextIndexToPut)
				logModified = true
				if rf.committedIndex >= nextIndexToPut {
					myDebug(rf.me, " bug? a commited index is cutted due to Log term mismatch ", rf.committedIndex, nextIndexToPut)
				}
				break
			}
			nextEntryToPut++
		}
		for ; nextEntryToPut < len(args.Entries); nextEntryToPut++ {
			rf.appendEntry(&args.Entries[nextEntryToPut])
			logModified = true
		}
		reply.Success = true
		if len(args.Entries) != 0 {
			myDebug(rf.me, "<-", args.LeaderId, " :after appending, the lastidx,lastterm=", rf.logLength()-1, rf.getTerm(rf.logLength()-1))
		}
		if args.LeaderCommit > rf.committedIndex {
			if args.LeaderCommit < rf.logLength() - 1 {
				myDebug(rf.me, " catch up committedIndex to ", args.LeaderCommit)
				rf.committedIndex = args.LeaderCommit
			} else {
				myDebug(rf.me, " catch up committedIndex to ", rf.logLength()-1)
				rf.committedIndex = rf.logLength() - 1
			}
		} else {
			if len(args.Entries) != 0 {
				myDebug(rf.me, "<-", args.LeaderId, " :after appending, cmtIdx remains. leadercommit,cmtIdx=", args.LeaderCommit, rf.committedIndex)
			}
		}
		if logModified {
			rf.persist()
		}
	}

	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term               int
	LeaderId           int
	LastIncludedIndex  int
	LastIncludedTerm   int
	Data               []byte
}

type InstallSnapshotReply struct {
	Term  int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	myDebug(rf.me, "recv installSnapshot rpc, args=(", args.Term, args.LeaderId, 
	        args.LastIncludedIndex, args.LastIncludedTerm, ")")
	rf.lastLeaderTS = time.Now().UnixNano()
	reply.Term = args.Term

	if rf.currentTerm == args.Term && rf.role == Follower && rf.leader != -1 {
		// nothing to modify here
		if (rf.leader != args.LeaderId) {
			panic("Bug detected: different leader for same term")
		}
	} else if rf.currentTerm < args.Term {
		// catch up the new term
		rf.votedFor = -1
		myDebug(rf.me, "<-", args.LeaderId, " :append, move term from ", 
						rf.currentTerm, " to ", args.Term)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.leader = args.LeaderId
		rf.lastLeaderTS = time.Now().UnixNano()
		rf.persist()
	} else {
		// maybe need to learn the current term's leader, give up election
		rf.lastLeaderTS = time.Now().UnixNano()
		rf.role = Follower
		rf.leader = args.LeaderId
		myDebug(rf.me, "<-", args.LeaderId, " :append term checked.", 
						" may give up election in the same term=", args.Term)
	}

	if rf.lastIndexInSnapshot >= args.LastIncludedIndex {
		// I already have this or newer snapshot, return.
		rf.mu.Unlock()
		return
	}

	// args.Data contains the entire snapshot, including lastIncludedIndex and term
	rf.persistWithEntireSnapshot(args.Data)

	if rf.logLength()-1 <= args.LastIncludedIndex {
		rf.log = make([]Entry, 0)
	} else {
		newlog := make([]Entry, 0)
		for i := args.LastIncludedIndex + 1; i < rf.logLength(); i++ {
			newlog = append(newlog, *rf.getEntry(i))
		}
		rf.log = newlog
	}

	// if rf.lastApplied < args.LastIncludedIndex {
	rf.lastApplied = args.LastIncludedIndex
	// }

	rf.lastIndexInSnapshot = args.LastIncludedIndex
	rf.lastTermInSnapshot  = args.LastIncludedTerm

	rf.newSnapshotInstalled = true
	snapDebug(rf.me, "snapshot installed, newinstalled=true, lastIdx, lastApplied=", args.LastIncludedIndex, rf.lastApplied)
	rf.mu.Unlock()
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

	// Your code here (2B).
	rf.mu.Lock()
	
	if rf.role != Leader{
		rf.mu.Unlock()
		return -1, -1, false
	}

	index := rf.logLength()
	term := rf.currentTerm

	rf.appendEntry(&Entry{command, term, clientDataEntryType})
	rf.persist()
	myDebug(rf.me, " append a cmd from client, now len(Log)=", rf.logLength())
	rf.mu.Unlock()
	go func() {
		select {
		case rf.appendNoticeChan <- nil:
		default:
		}
	} ()

	return index, term, true
}

// Kill :
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.terminate = true
	rf.mu.Unlock()
}

// DoSnapshot :
func (rf *Raft) DoSnapshot(kvsnapshot []byte, executedIndex int) {
	rf.mu.Lock()
	
	if rf.logLength() - 1 < executedIndex || rf.lastApplied < executedIndex {
		snapDebug(rf.me, "executed an entry I never send out, lenlog,lastapplied,executedidx=",
	            rf.logLength(), rf.lastApplied, executedIndex)
		panic("executed an entry I never send out")
	}

	if rf.lastIndexInSnapshot >= executedIndex {
		rf.mu.Unlock()
		return
	}
	
	theTerm := rf.getTerm(executedIndex)
	rf.persistWithSMSnapshot(kvsnapshot, executedIndex, theTerm)
	
	newlog := make([]Entry, 0)
	for i := executedIndex + 1; i < rf.logLength(); i++ {
		newlog = append(newlog, *rf.getEntry(i))
	}
	rf.log = newlog

	rf.lastIndexInSnapshot = executedIndex
	rf.lastTermInSnapshot  = theTerm
	rf.mu.Unlock()
}

func tryNotice(Chan chan interface {}) {
	select {
	case Chan <- nil:
	default:
	}
}

// lock before call this
func (rf *Raft) prepareAppendEntriesArgs(i int) *AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i]-1
	args.PrevLogTerm = rf.getTerm(rf.nextIndex[i] - 1)
	if rf.logLength() - 1 < rf.nextIndex[i] {
		// myDebug(rf.me, "sending out heart beat to ", i)
		args.Entries = nil
	} else {
		myDebug(rf.me, "->", i, ": meaningful append(t, pT, pI, leaderCmt)=", 
						args.Term, args.PrevLogTerm, args.PrevLogIndex, rf.committedIndex)
		if (rf.nextIndex[i] + maxAppendEntries > rf.logLength()) {
			args.Entries = rf.getLogSlice(rf.nextIndex[i], -1)
		} else {
			args.Entries = rf.getLogSlice(rf.nextIndex[i], rf.nextIndex[i] + maxAppendEntries)
		}
		myDebug(rf.me, "->", i, ": meaningful append(t, pT, pI, leaderCmt,lastI,lastT)=", 
						args.Term, args.PrevLogTerm, args.PrevLogIndex, rf.committedIndex,
						rf.nextIndex[i]+len(args.Entries)-1, args.Entries[len(args.Entries)-1].Term)
	}
	args.LeaderCommit = rf.committedIndex
	return &args
}

// return value is should return the loop
func (rf *Raft) handleAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply, nextIndexDecStep *int, i int) {
	if reply.Term > args.Term || rf.currentTerm > args.Term {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.leader = -1
			rf.role = Follower
			// rf.lastLeaderTS = time.Now().UnixNano()
			rf.persist()
		}
		// rf.mu.Unlock()
	} else if reply.Success {
		*nextIndexDecStep = 1
		if args.PrevLogIndex == rf.nextIndex[i]-1 {
			rf.nextIndex[i]+= len(args.Entries)
		}
		if rf.nextIndex[i] - 1 > rf.matchIndex[i] {
			rf.matchIndex[i] = rf.nextIndex[i] -1
		}
		ci := findCommitIndex(rf.matchIndex)
		if ci > rf.committedIndex {
			if rf.getTerm(ci) >= rf.currentTerm {
				myDebug(rf.me, " move committedIndex to ", ci)
				rf.committedIndex = ci
			}
		}
	} else {
		if args.PrevLogIndex == rf.nextIndex[i]-1 {
			orig := rf.nextIndex[i]
			// rf.nextIndex[i] -= *nextIndexDecStep
			if rf.nextIndex[i] > rf.lastIndexInSnapshot + 1 {
				if rf.nextIndex[i] - *nextIndexDecStep <= rf.lastIndexInSnapshot {
					rf.nextIndex[i] = rf.lastIndexInSnapshot + 1
				} else {
					rf.nextIndex[i] = rf.nextIndex[i] - *nextIndexDecStep
					*nextIndexDecStep *= 2
				}
			} else if rf.nextIndex[i] == rf.lastIndexInSnapshot + 1 {
				snapDebug(rf.me, "append to", i, "failed, previdx=", args.PrevLogIndex, ", move nextIdx to lastIdxInSnapshot=", rf.lastIndexInSnapshot)
				// we must send installsnapshot
				rf.nextIndex[i] = rf.lastIndexInSnapshot
			} else {
				// last time we have rf.nextIndex > rf.lastindexInSnapshot, but at this
				// point of reply, the snapshot has already move on.
				// so we just let me send installsnapshot in the next round
			}
			if rf.nextIndex[i] <= 0 {
				rf.nextIndex[i] = 1
			}
			myDebug(rf.me, " dec nextIndex of ", i, " from ", orig, " to ", rf.nextIndex[i])
		}
	}
}

func (rf *Raft) prepareInstallSnapshotArgs(i int) *InstallSnapshotArgs {
	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.lastIndexInSnapshot
	args.LastIncludedTerm = rf.lastTermInSnapshot
	args.Data = rf.persister.ReadSnapshot()
	return &args
}

func (rf *Raft) handleInstallSnapshotreply(args *InstallSnapshotArgs, reply *InstallSnapshotReply, i int) {
	if reply.Term > args.Term || rf.currentTerm > args.Term {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.leader = -1
			rf.role = Follower
			// rf.lastLeaderTS = time.Now().UnixNano()
			rf.persist()
		}
		// rf.mu.Unlock()
		return
	}
	// now we say it is successful
	if args.LastIncludedIndex == rf.lastIndexInSnapshot && rf.nextIndex[i] <= rf.lastIndexInSnapshot {
		// everything is the same
		rf.nextIndex[i] = rf.lastIndexInSnapshot + 1
		if rf.nextIndex[i] - 1 > rf.matchIndex[i] {
			rf.matchIndex[i] = rf.nextIndex[i] - 1
		}
		ci := findCommitIndex(rf.matchIndex)
		if ci > rf.committedIndex {
			if rf.getTerm(ci) >= rf.currentTerm {
				myDebug(rf.me, " move committedIndex to ", ci)
				rf.committedIndex = ci
			}
		}
	}
}

func (rf *Raft) leaderProdecure() {
	// go rf.heartBeatProcedure()
	me := rf.me
	appendNoticeChanList := make([]chan interface{}, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if me == i {
			continue
		}
		appendNoticeChanList[i] = make(chan interface{})

		go func(i int) {
			// this routine constantly check the nextIndex, matchIndex
			nextIndexDecStep := 1
			for {
				<- appendNoticeChanList[i]
				rf.mu.Lock()
				if rf.terminate {
					rf.mu.Unlock()
					return
				}
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}
				go func() { // one routine for one rpc call
					if rf.nextIndex[i] <= rf.lastIndexInSnapshot {
						// we should send install snapshot
						nextIndexDecStep = 1
						args := rf.prepareInstallSnapshotArgs(i)
						rf.mu.Unlock()

						reply := &InstallSnapshotReply{}
						ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)
						if !ok {
							return
						}

						rf.mu.Lock()
						rf.handleInstallSnapshotreply(args, reply, i)
						rf.mu.Unlock()
					} else {
						// se should send append entries
						args := rf.prepareAppendEntriesArgs(i)
						rf.mu.Unlock()
						
						reply := &AppendEntriesReply{}
						ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
						if !ok {
							return
						}
						
						rf.mu.Lock()
						rf.handleAppendEntriesReply(args, reply, &nextIndexDecStep, i)
						rf.mu.Unlock()
					}
				} ()
			}
		} (i)
	}
	go func() {
		// a routine that constantly try to notify leader to send out appendEntries
		for {
			rf.mu.Lock()
			role := rf.role
			terminate := rf.terminate
			rf.mu.Unlock()
			if terminate {
				return
			}
			if role != Leader {
				tryNotice(rf.appendNoticeChan)
				return
			}
			tryNotice(rf.appendNoticeChan)
			time.Sleep(time.Duration(heartBeatDelta))
		}
	} ()
	go func() {
		// a routine that read from prev routine and distribute notifications to routine 
		// for each other peer
		for {
			rf.mu.Lock()
			role := rf.role
			terminate := rf.terminate
			rf.mu.Unlock()
			if terminate {
				return
			}
			if role != Leader {
				for i:=0; i < len(rf.peers); i++ {
					if i == me {
						continue
					}
					tryNotice(appendNoticeChanList[i])
				}
				return
			}
			<-rf.appendNoticeChan
			for i:=0; i < len(rf.peers); i++ {
				if i == me {
					continue
				}
				tryNotice(appendNoticeChanList[i])
			}
		}
	} ()

}


func (rf *Raft) electionProcedure() {
	for {
		rf.mu.Lock()
		if rf.terminate {
			rf.mu.Unlock()
			return
		}
		if rf.role != Candidate {
			myDebug(rf.me, " terminate election due to term changed by some routine else")
			rf.lastLeaderTS = time.Now().UnixNano()
			rf.mu.Unlock()
			return
		}
		rf.currentTerm ++
		thisTerm := rf.currentTerm
		rf.votedFor = rf.me
		rf.persist()
		requestVoteArgs := RequestVoteArgs{}
		requestVoteArgs.LastLogIndex = rf.logLength() - 1
		requestVoteArgs.LastLogTerm = rf.getTerm(requestVoteArgs.LastLogIndex)
		rf.mu.Unlock()
		n := len(rf.peers)
		requestVoteArgs.Term = thisTerm
		requestVoteArgs.CandidateId = rf.me
		myDebug(rf.me, " electing in term ", thisTerm)
		votesOfThisTerm := make(chan int, n-1)
		rejectedVotesByLogOfThisTerm := make(chan int, 1)
		rejectedVotesByLogOfThisTerm <- 0
		
		for i:=0; i < n; i++ {
			if i == rf.me {
				continue
			}
			requestVoteReply := &RequestVoteReply{}
			go func(i int) {
				myDebug(rf.me, "->", i, " :sending vote ")
				ok := rf.sendRequestVote(
					i, &requestVoteArgs, requestVoteReply)
				// should do some callback to count majority
				if !ok {
					return
				}
				if requestVoteReply.VoteGranted{
					myDebug(rf.me, "->", i, " :recv vote grant of term", thisTerm, ", put to chan")
					votesOfThisTerm <- i
				} else {
					if requestVoteReply.Term > thisTerm {
						rf.mu.Lock()
						if requestVoteReply.Term > rf.currentTerm {
							myDebug(rf.me, " is stale, move from candidate in ", rf.currentTerm, 
											" to follower in ", requestVoteReply.Term)
							rf.role = Follower
							rf.currentTerm = requestVoteReply.Term
							rf.votedFor = -1
							rf.lastLeaderTS = time.Now().UnixNano()
							rf.leader = -1
							rf.persist()
						}
						rf.mu.Unlock()
					} else {
						orig := <-rejectedVotesByLogOfThisTerm
						rejectedVotesByLogOfThisTerm <- orig+1
					}
				}
			} (i)
		}
		go func() {
			rand.Seed(time.Now().UnixNano())
			time.Sleep(time.Duration(rand.Int63n(electionTimeoutRange) + electionTimeoutBase))
			myDebug(rf.me, " election timelimit, election count chan <- -1, t=", thisTerm)
			votesOfThisTerm <- -1
		} ()

		voteReceived := 0
		for {
			i := <-votesOfThisTerm
			if i == -1 {
				myDebug(rf.me, " election term ", thisTerm, " timeout, reelecting in next term")
				rejected := <- rejectedVotesByLogOfThisTerm
				rejectedVotesByLogOfThisTerm <- rejected
				if rejected > n/2 {
					// myDebug(rf.me, " majority rejected me in this election term ", thisTerm)
					// rf.mu.Lock()
					// rf.lastLeaderTS = time.Now().UnixNano()
					// rf.mu.Unlock()
					// return
				}
				// election failed, reelection
				break
			}

			voteReceived ++
			myDebug(rf.me, "->", i, " recv granted vote of term", thisTerm)
			if voteReceived >= n / 2 {
				rf.mu.Lock()
				// election succeed
				if rf.currentTerm > thisTerm {
					myDebug(rf.me, "->", i, " recv grant vote in term ", thisTerm, ", but I've moved on")
					// election succeed, but this server has already stale and move to next term
					rf.mu.Unlock()
					rf.lastLeaderTS = time.Now().UnixNano()
					return
				}
				myDebug(rf.me, " has f+1 votes and becomes leader of term ", thisTerm)
				rf.role = Leader
				// start heart beat procedure
				rf.leader = rf.me
				// rf.appendEntry(&Entry{nil, thisTerm, noopEntryType})
				rf.matchIndex = make([]int, len(rf.peers))
				rf.nextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.logLength()
				}
				rf.mu.Unlock()
				go func (rf *Raft) {
					rf.leaderProdecure()
				} (rf)
				return
			}
		}
	}
}

// Make :
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
	// rf.applyChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.lastIndexInSnapshot = -1
	rf.lastTermInSnapshot  = -1
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{nil, 0, noopEntryType}
	rf.committedIndex = 0
	rf.lastApplied = 0
	rf.lastLeaderTS = 0
	rf.leader = -1
	rf.terminate = false
	rf.appendNoticeChan = make(chan interface{})
	rf.newSnapshotInstalled = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	

	go func () {
		for {
			time.Sleep(time.Duration(leaderCheckDelta))
			rf.mu.Lock()
			role := rf.role
			terminate := rf.terminate
			rf.mu.Unlock()
			if terminate {
				return
			}
			if role != Follower {
				continue
			}
			now := time.Now().UnixNano()
			rf.mu.Lock()
			if now - rf.lastLeaderTS > leaderStepDownDelta {
				myDebug(rf.me, " thinks leader is down, start election")
				// start election
				rf.role = Candidate
				rf.mu.Unlock()
				rf.electionProcedure()
			} else {
				rf.mu.Unlock()
			}
		}
	} ()

	go applyRoutine(rf, applyCh)
	return rf
}

func applyRoutine(rf *Raft, applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.terminate {
			rf.mu.Unlock()
			return
		}
		// rf.mu.Unlock()
		// rf.mu.Lock()
		if rf.newSnapshotInstalled {
			if rf.lastApplied < rf.lastIndexInSnapshot {
				panic("some log in snapshot is not applied")
			} else if rf.lastApplied == rf.lastIndexInSnapshot {
				// send the snapshot to kv
				applyMsg := ApplyMsg{}
				applyMsg.CommandValid = false
				// snapshot := 
				rbuf := bytes.NewBuffer(rf.persister.ReadSnapshot())
				rf.mu.Unlock()
				decoder := labgob.NewDecoder(rbuf)
				decoder.Decode(&applyMsg.Snapshot)
				if applyMsg.Snapshot == nil {
					panic("nil snapshot")
				}
				applyCh <- applyMsg
				rf.newSnapshotInstalled = false
			} else {
				panic("after applied some new entry, the newSnapShotInstalled is still new")
			}

		} else if rf.committedIndex > rf.lastApplied {
			rf.lastApplied ++
			entry := rf.getEntry(rf.lastApplied)
			applyMsg := ApplyMsg{}
			applyMsg.CommandIndex = rf.lastApplied
			applyMsg.Command = entry.Data
			applyMsg.PersistStateSize = rf.persister.RaftStateSize()
			rf.mu.Unlock()
			myDebug(rf.me, " sending apply msg of idx=", applyMsg.CommandIndex)
			// println(rf.me, " sending apply msg of idx=", applyMsg.CommandIndex)

			switch entry.EntryType {
			case clientDataEntryType:
				applyMsg.CommandValid = true
			case noopEntryType:
				// applyMsg.CommandValid = true
				panic(" unused noop entry type")
			default :
				panic(" unused other entry type")
				// applyMsg.CommandValid = false
			}
			applyCh <- applyMsg
		} else {
			rf.mu.Unlock()
			time.Sleep(time.Duration(applyCheckDelta))
		}
	}
}
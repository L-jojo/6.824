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
	"6.824/labrpc"
	"math"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

//some constant
const HEARTBEAT float64 = 150
const TIMEOUTLOW float64 = 500
const TIMEOUTHIGH float64 = 1000
const CHECKPERIOD float64 = 300 //check timeout period
const CHECKAPPLYPERIOD float64 = 10
const CHECKAPPENDPERIOD float64 = 10
const CHECKCOMMITPERIOD float64 = 10
const FOLLOWER int = 2
const CANDIDATE int = 1
const LEADER int = 0

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type LogEntry struct {
	Term    int
	Command interface{}
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int       // 0 - leader , 1 - candidate, 2 - follower
	commitIndex int       // lately committed log
	lastApplied int       // index of highest log entry applied to state machine
	timestamp   time.Time //last time receive the leader's heartbeat
	cond        *sync.Cond
	applyCh     chan ApplyMsg
	currentTerm int        // term of this peer
	votedFor    int        //candidateId that received vote in current term
	log         []LogEntry //every log contains command and term
	voteNum     int        //current vote num
	// 选举后重新初始化, leader需要保存的
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //index of log entry immediately preceding new ones
	PrevLogTerm  int //term of prevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int //leader's commitIndex
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

//当rf的commitIndex更新时，会
func (rf *Raft) applyCommitted() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}

		rf.lastApplied++
		DPrintf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service", rf.currentTerm, rf.me, rf.state, rf.lastApplied)

		cmdIndex := rf.lastApplied
		command := rf.log[cmdIndex].Command

		msg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: cmdIndex,
		}
		//这个channel可能阻塞
		rf.applyCh <- msg
		rf.mu.Unlock()
		DPrintf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service successfully", rf.currentTerm, rf.me, rf.state, rf.lastApplied)
		time.Sleep(time.Duration(CHECKAPPLYPERIOD) * time.Millisecond)
	}
}
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()

	DPrintf("[term %d]: Raft[%d] [state %d] receive AppendEntries from Raft[%d]", rf.currentTerm, rf.me, rf.state, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER //可能是废话
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	rf.timestamp = time.Now()

	// reply false if log doesn't contain an entry at preLogIndex whose term matches preLogTerm
	// remember to handle the case where prevLogIndex points beyond the end of your log
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	i := args.PrevLogIndex + 1
	j := 0
	isFinished := false
	for j := 0; j < len(args.Entries); j++ {
		if i+j >= len(rf.log) { //超了先跳出
			break
		}
		if rf.log[i+j].Term == args.Entries[j].Term {
			continue
		} else {
			rf.log = append(rf.log[:i+j], args.Entries[j:]...)
			isFinished = true
			break
		}
	}
	if !isFinished && j < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[j:]...)
	}
	//DPrintfLog(rf)
	//5.
	if args.LeaderCommit > rf.commitIndex {
		oriCommitIndex := rf.commitIndex
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
		DPrintf("[term %d]:Raft [%d] [state %d] commitIndex is %d", rf.currentTerm, rf.me, rf.state, rf.commitIndex)
		if rf.commitIndex > oriCommitIndex {
			//唤醒replycommit进程
			rf.cond.Broadcast()
		}
	}
	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntris(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return reply.Success && ok
}

func (rf *Raft) callSendAppendEntris(server int, term int, leaderId int, preLogIndex int, preLogTerm int, entris []LogEntry, leaderCommit int) bool {
	//DPrintf("[term %d]: Raft [%d] sends appendEntry to server [%d]", term, leaderId, server)
	args := AppendEntryArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      entris,
		LeaderCommit: leaderCommit,
	}
	reply := AppendEntryReply{}
	reply.Success = rf.sendAppendEntris(server, &args, &reply)
	DPrintf("[term %d]: Raft [%d] sends successsfully appendEntry to server [%d]", term, leaderId, server)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term != rf.currentTerm {
		reply.Success = false
	}
	if reply.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = reply.Term
	}
	return reply.Success
}

func (rf *Raft) leaderHeartbeat() {
	DPrintf("[term :%d]: Raft [%d] becomes a Leader!!!!", rf.currentTerm, rf.me)
	for {
		rf.mu.Lock()
		if rf.state != LEADER || rf.killed() {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		leaderCommit := rf.commitIndex
		preLogIndex := len(rf.log) - 1
		preLogTerm := rf.log[preLogIndex].Term
		rf.mu.Unlock()

		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.callSendAppendEntris(server, term, rf.me, preLogIndex, preLogTerm, make([]LogEntry, 0), leaderCommit)
			}(server)
		}
		time.Sleep(time.Millisecond * time.Duration(HEARTBEAT))
	}
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
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER || rf.killed() {
		return index, term, false
	}
	DPrintf("[term %d]:Raft [%d] start consensus, receive a command", rf.currentTerm, rf.me)
	index = len(rf.log)
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	return index, term, true
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//periodically check if there exists a log entry which is commited
func (rf *Raft) commitChecker() {
	for {
		consensus := 1

		rf.mu.Lock()
		//DPrintf("[term %d]: Raft %d, the length of rf.log is %d, the commitindex is %d", rf.currentTerm, rf.me, len(rf.log)-1, rf.commitIndex)
		if len(rf.log)-1 > rf.commitIndex {
			N := rf.commitIndex + 1
			for i := 0; i < len(rf.peers); i++ {
				//DPrintfWte("[term %d]:Raft %d 's loglength is %d, nextIndex is %d, matchIndex is %d", rf.currentTerm, i, len(rf.log), rf.nextIndex[i], rf.matchIndex[i])
				if rf.matchIndex[i] >= N {
					consensus++
				}
			}
			if consensus*2 > len(rf.peers) && rf.log[N].Term == rf.currentTerm {
				DPrintf("[term %d]:Raft [%d] [state %d] commit log entry %d successfully", rf.currentTerm, rf.me, rf.state, N)
				rf.commitIndex = N
				rf.cond.Broadcast()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(CHECKCOMMITPERIOD))
	}
}

// periodically check if last log index >= nextIndex[server], if so, send AppendEntries (leader only)
func (rf *Raft) appendChecker(server int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		nxtId := rf.nextIndex[server]
		term := rf.currentTerm

		//if nxtId > len(rf.log)-1 {
		//	rf.mu.Unlock()
		//	return
		//}

		if len(rf.log)-1 >= nxtId {
			entris := rf.log[nxtId:]
			DPrintf("[term %d]:Raft[%d] append entries to Raft [%d]", rf.currentTerm, rf.me, server)
			success := rf.callSendAppendEntris(server, rf.currentTerm, rf.me, nxtId-1, rf.log[nxtId-1].Term, entris, rf.commitIndex)
			rf.mu.Lock()
			if rf.currentTerm != term {
				rf.mu.Unlock()
				continue
			}
			//DPrintfWte("%t", success)
			if success {
				rf.nextIndex[server] += len(entris)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				//DPrintfWte("[term :%d]:Leader %d append to Raft %d success, now raft's nextIndex is %d, matchIndex is %d, Leader's Log length is %d", rf.currentTerm, rf.me, server, rf.nextIndex[server], rf.matchIndex[server], len(rf.log))
				DPrintf("[term :%d]:Raft [%d] append entries to Raft %d successfully", rf.currentTerm, rf.me, server)

			} else {
				rf.nextIndex[server] = int(math.Max(1.0, float64(rf.nextIndex[server]-1)))
			}
			rf.mu.Unlock()
		}
		time.Sleep(time.Millisecond * time.Duration(CHECKAPPENDPERIOD))
	}
}

func (rf *Raft) allocateAppendCheckers() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.appendChecker(i)
	}
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
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.cond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.timestamp = time.Now()
	rf.state = FOLLOWER
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionChecker()
	go rf.applyCommitted()
	//go rf.ticker()

	return rf
}

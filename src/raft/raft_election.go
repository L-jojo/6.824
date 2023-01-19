package raft

import (
	"math/rand"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool //true means candidate received vote
	Term        int  //currentTerm
}
type PreRequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId int
	Term        int
}

type PreRequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool //true means candidate received vote
	Term        int  //currentTerm
}

func (rf *Raft) PreRequestVote(args *PreRequestVoteArgs, reply *PreRequestVoteReply) {
	rf.mu.Lock()
	DPrintf("[term %d]: Raft[%d] receive preRequestVote from Raft[%d]", rf.currentTerm, rf.me, args.CandidateId)
	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.mu.Unlock()
}

func (rf *Raft) SendPreRequestVote(server int, args *PreRequestVoteArgs, reply *PreRequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.PreRequestVote", args, reply)
	return ok
}

func (rf *Raft) CallSendPreRequestVote(server int, candidate int, term int) bool {
	rf.mu.Lock()
	args := PreRequestVoteArgs{
		CandidateId: rf.me,
		Term:        rf.currentTerm,
	}
	reply := PreRequestVoteReply{}
	rf.mu.Unlock()
	ok := rf.SendPreRequestVote(server, &args, &reply)
	if ok {
		return reply.VoteGranted
	}
	return false
}

func (rf *Raft) StartPreRequest() bool {
	if rf.state == LEADER || rf.killed() {
		return false
	}
	var preVoteMutex sync.Mutex
	preVoteFinished := false
	vote := 1

	for i := range rf.peers {
		if i == rf.me {
			DPrintfWte("[term :%d]:Raft %d preVote for itself", rf.currentTerm, rf.me)
			continue
		}
		go func(i int) {
			voteGranted := rf.CallSendPreRequestVote(i, rf.me, rf.currentTerm)
			preVoteMutex.Lock()
			if voteGranted {
				vote++
				DPrintfWte("[term :%d]:Raft %d preVote for Raft %d", rf.currentTerm, i, rf.me)
				if vote*2 >= len(rf.peers) && !preVoteFinished {
					preVoteFinished = true
					DPrintfWte("[term %d]:Raft %d preVoteSuccessful!", rf.currentTerm, rf.me)
				}
			}
			preVoteMutex.Unlock()
		}(i)
	}
	return preVoteFinished
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("[term %d]: Raft[%d] receive requestVote from Raft[%d]", rf.currentTerm, rf.me, args.CandidateId)
	lastlogterm := rf.log[len(rf.log)-1].Term
	//logComplete := (args.LastLogTerm > lastlogterm || (args.LastLogIndex >= len(rf.log)-1 && args.LastLogTerm == lastlogterm))
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	//other server has higher term !

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	//Term 一致时， 未投票给别人或投票设置为请求者本人 5.4.1 restrict election
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > lastlogterm ||
			(args.LastLogIndex >= len(rf.log)-1 && args.LastLogTerm == lastlogterm) {
			reply.VoteGranted = true
			rf.timestamp = time.Now()
			rf.votedFor = args.CandidateId
			DPrintf("[term %d]: Raft [%d] vote for Raft[%d]", rf.currentTerm, rf.me, rf.votedFor)
			rf.mu.Unlock()
			return
		}
	}
	reply.VoteGranted = false
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//真正发送请求投票RPC， 构造传入参数（term，log等），server为请求对象
func (rf *Raft) callSendRequestVote(server int, term int, lastLogIndex int, lastLogTerm int) bool {
	DPrintf("[term %d]: Raft [%d] sends requestvote RPC to server [%d]", term, rf.me, server)
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// *** to avoid term confusion !!! ***
	if term != rf.currentTerm {
		return false
	}

	//other server has higher term
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	return reply.VoteGranted
}

func (rf *Raft) startElection() bool {
	//preReady := rf.StartPreRequest()
	//if !preReady {
	//	return false
	//}
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.currentTerm++
	lastLogIndex := len(rf.log) - 1
	lastlogterm := rf.log[lastLogIndex].Term

	rf.state = CANDIDATE
	rf.mu.Unlock()
	DPrintf("[term: %d]: Raft [%d] start election", rf.currentTerm, rf.me)
	term := rf.currentTerm
	vote := 1 //自己给自己投的票
	electionFinished := false
	var voteMutex sync.Mutex

	for server, _ := range rf.peers {
		if server == rf.me {
			DPrintf("[term: %d] Raft:[%d] vote for self", rf.currentTerm, rf.me)
			continue
		}
		go func(server int) {
			voteGranted := rf.callSendRequestVote(server, term, lastLogIndex, lastlogterm)
			voteMutex.Lock()
			if voteGranted && !electionFinished {
				vote++
				DPrintf("[term: %d] Raft:[%d] vote for Raft[%d]", rf.currentTerm, server, rf.me)
				if vote*2 >= len(rf.peers) {
					electionFinished = true
					rf.mu.Lock()
					rf.state = LEADER
					rf.timestamp = time.Now()
					//重新初始化nextIndex与matchIndex数组
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					go rf.leaderHeartbeat()
					go rf.commitChecker()
					go rf.allocateAppendCheckers()
				}
			}
			voteMutex.Unlock()
		}(server)
	}
	return electionFinished
}

func (rf *Raft) electionChecker() {
	for {
		r := rand.New(rand.NewSource(int64(rf.me)))
		//rf.mu.Lock()
		//check if dead

		if rf.killed() {
			break
		}
		electionLock := sync.Mutex{}
		electionLock.Lock()
		timeout := int(float64(r.Float64())*(TIMEOUTHIGH-TIMEOUTLOW) + TIMEOUTLOW)
		//if timeout and the server is not a leader, start election
		//DPrintfWte("[term %d]: Raft[%d] has not receive append or heartbeat for %d", rf.currentTerm, rf.me, time.Since(rf.timestamp).Milliseconds())
		if rf.state != LEADER && time.Since(rf.timestamp) > time.Duration(timeout)*time.Millisecond {
			go rf.startElection()
		}
		electionLock.Unlock()
		//rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(CHECKPERIOD))
	}
}

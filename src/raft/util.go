package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func DPrintfLog(raft *Raft) {
	DPrintfWte("raft [%d]'s detail is following: currentTerm is %d ; state is %d ; log length is %d, commit index is %d ; ", raft.me, raft.currentTerm, raft.state, len(raft.log), raft.commitIndex)
}

func DPrintfWte(format string, a ...interface{}) {
	log.Printf(format, a...)
	return
}

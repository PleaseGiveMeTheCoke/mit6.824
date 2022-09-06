package raft

import (
	"fmt"
	"log"
	"strconv"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func RaftToString(x int) string {
	return strconv.Itoa(x)
}

func (rf *Raft) RaftPrint(msg string) {

	if 2 == 1 && !rf.killed() {
		fmt.Println("Raft id = " + strconv.Itoa(rf.me) + " term = " + strconv.Itoa(rf.currentTerm) + " : " + msg)
	}
}
func (rf *Raft) RaftPrintB(msg string) {

	if 2 == 1 && !rf.killed() {
		fmt.Printf("Raft id = " + strconv.Itoa(rf.me) + " term = " + strconv.Itoa(rf.currentTerm) + " lastIncludedIndex = " + RaftToString(rf.lastIncludedIndex) + " : " + msg)
	}
}
func (rf *Raft) RaftPrintC(msg string) {

	if 2 == 1 && !rf.killed() {
		fmt.Printf("Raft id = " + strconv.Itoa(rf.me) + " term = " + strconv.Itoa(rf.currentTerm) + " lastIncludedIndex = " + RaftToString(rf.lastIncludedIndex) + " : " + msg)
	}
}
func (rf *Raft) RaftPrintf(msg string, a ...interface{}) {

	if 2 == 1 && !rf.killed() {
		fmt.Printf("Raft id = "+strconv.Itoa(rf.me)+" term = "+strconv.Itoa(rf.currentTerm)+" lastIncludedIndex = "+RaftToString(rf.lastIncludedIndex)+" : "+msg+"\n", a)
	}

}
func (rf *Raft) RaftPrintD(msg string) {

	if 2 == 1 && !rf.killed() {
		fmt.Printf("Raft id = " + strconv.Itoa(rf.me) + " term = " + strconv.Itoa(rf.currentTerm) + " lastIncludedIndex = " + RaftToString(rf.lastIncludedIndex) + " : " + msg)
	}
}

func myPrint(msg string) {
	if 2 == 1 {
		fmt.Println("==================" + msg)
	}
}

func myPrintf(msg string, a ...interface{}) {

	if 2 == 1 {
		fmt.Printf("================="+msg+"\n", a)
	}

}

package raft

import (
	"math/rand"
	"time"
)

type SnapshotArgs struct { // 快照同步
	Term 			  int // Leader的term
	LeaderId 		  int // Leader的id
	LastIncludedIndex int //快照包含的最后的日志indx
	LastIncludedTerm  int //快照包含的最后的日志term
	Data              []byte //快照原始字节流数据
	// Offset 本Lab不实现
	// Done 本Lab不实现
}

type SnapshotReply struct { // 快照同步结果
	Term int // 节点的任期
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
//防止快照和日志applyCh时冲突
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// 废弃，返回true即可

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex < rf.lastLogIndex {
		return false
	}
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 上层service向rf节点发来更新快照请求，请求保存index之前的日志作为快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return 
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 判断是否更新快照
	// 1、这个index已经保存在最新快照内，无需更新
	// 2、index尚未被committed，不能被保存为快照
	if index <= rf.lastLogIndex || index > rf.commitIndex {
		return 
	}

	CPrintf("更新快照")
	// 更新快照
	lastLogTerm := rf.getLogByIndex(index).Term

	CPrintf("%v日志保存为快照前,日志长度:%v,rf.lastLogIndex:%v",rf.me, len(rf.log), rf.lastLogIndex)
	rf.log = rf.cutLogFrom(index + 1)
	rf.log = append([]LogEntry{}, rf.log...) // 垃圾回收
	CPrintf("%v日志保存为快照后,日志长度:%v,rf.lastLogIndex:%v",rf.me, len(rf.log), index)
	// 更新最新快照相关
	rf.lastLogIndex = index
	rf.lastLogTerm = lastLogTerm
	CPrintf("%v更新快照相关:rf.lastLogIndex:%v, rf.lastLogTerm:%v",rf.me, rf.lastLogIndex, rf.lastLogTerm)


	// 更新commitIndex、lastApplied
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	//持久化日志
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// leader发送follower同步快照请求
func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) {
	if rf.killed() {
		return 
	}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	CPrintf("%v同步快照给%v", rf.me, server)
	for !ok {
		return 
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1 //更新投票相关
		rf.persist()
		rf.votedmeNum = 0
		CPrintf("%v由Leader转变为Follower",rf.me)
	}

	// 更新nextIndex、matchIndex
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
}

// follower收到leader的同步快照请求
func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) error {
	if rf.killed() {
		return nil
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Leader { // 收到快照转变身份，Leader只发不收
		rf.role = Follower
	}

	// 更新currentTerm
	rf.currentTerm = max(rf.currentTerm, args.Term)
	rf.persist()
	reply.Term = rf.currentTerm
	
	if rf.currentTerm > args.Term {
		return nil
	}

	// 过期快照
	if args.LastIncludedIndex < rf.commitIndex || rf.lastLogIndex >= args.LastIncludedIndex{
		return nil
	}
	
	// 更新日志，日志变短
	CPrintf("%v同步更新快照前,日志长度:%v,rf.lastLogIndex:%v",rf.me, len(rf.log), rf.lastLogIndex)
	// len(rf.log):1 rf.lastLogIndex:0 args.LastIncludedInde:9 rf.getlastLogIndex():1
	// rf.logLen():1
	if rf.logLen() >= rf.lastLogIndex && args.LastIncludedIndex <= rf.getlastLogIndex() {
		rf.log = rf.cutLogFrom(args.LastIncludedIndex + 1)
	}else if rf.logLen() < args.LastIncludedIndex {
		rf.log = []LogEntry{}
	}
	CPrintf("%v同步更新快照后,日志长度:%v,rf.lastLogIndex:%v",rf.me, len(rf.log), args.LastIncludedIndex)
	// 更新最新快照相关
	rf.lastLogIndex = args.LastIncludedIndex
	rf.lastLogTerm = args.LastIncludedTerm
	CPrintf("%v更新快照相关:rf.lastLogIndex:%v, rf.lastLogTerm:%v",rf.me, rf.lastLogIndex, rf.lastLogTerm)
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	// 更新commitIndex、lastApplied
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastLogTerm,
		SnapshotIndex: rf.lastLogIndex,
	}
	if rf.lastApplied > rf.lastLogIndex { // 新增，为保证log consistent，再次检查
		return nil
	}
	rf.mu.Unlock()
	rf.applyChan <- msg
	rf.mu.Lock()
	rf.overtime = time.Duration(150 + rand.Intn(150)) * time.Millisecond //snapshot RPC也可以重置选举计时
	rf.timer.Reset(rf.overtime)
	return nil
}
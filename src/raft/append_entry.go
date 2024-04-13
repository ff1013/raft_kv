package raft

import (
	"math/rand"
	"time"
)


type AppendEntriesArgs struct { //AE消息,由Leader发出,AE可能是心跳，可以能是复制Leader日志
	Term          int          //Leader的任期
	LeaderId      int          //Leader的Id
	PrevLogIndex  int          //从哪个槽位之后开始追加
	PrevLogTerm   int          //要追加的槽位的前一任期term
	Entries       []LogEntry   //要储存的复制Leader的日志,心跳为空
	LeaderCommit  int          //被commit的日志槽位，即被大多数server都复制了的日志
}

type AppendEntriesReply struct { //AE消息的回复
	Term          int            //Leader的Term可能是过时的,Follower返回自己的给Leader更新
	Success       bool           //根据PrevLogIndex和PrevLogTerm判断是否返回正确
	//Fast Backup快速恢复(AE Success=fasle,返回一些信息用于Leader快速更新Follower)
	XTerm         int            //冲突的任期号
	XIndex        int            //任期号为XTEerm的第一条Log条目的槽位号
	XLen          int            //如果XTerm=-1出现空白槽时,开始加的位置
}

func (rf *Raft) appendEntries() {
	rf.timer.Reset(HeartBeatTimeout)
	for i := 0; i<len(rf.peers); i++ { //发送AE消息给其他的所有节点
		if i == rf.me {
			continue
		}
		// 要同步的内容已经保存到快照了
		if rf.nextIndex[i] <= rf.lastLogIndex && rf.lastLogIndex != 0{ 
			args := SnapshotArgs{
				Term:				rf.currentTerm,
				LeaderId:			rf.me,
				LastIncludedIndex:  rf.lastLogIndex,
				LastIncludedTerm:   rf.lastLogTerm,
				Data:				rf.persister.ReadSnapshot(),
			}
			reply := SnapshotReply{}
			CPrintf("rf.nextIndex[i]:%v, rf.lastLogIndex:%v", rf.nextIndex[i], rf.lastLogIndex)
			CPrintf("%v同步日志快照到%v", rf.me, i)
			go rf.sendInstallSnapshot(i, &args, &reply)
			continue
		}
		// 要同步的内容未保存到快照，按没有快照时的情况即可
		args := AppendEntriesArgs{ //考虑初始日志prev相关为空和心跳日志为空的情况
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm: 0,
			Entries: nil,
			LeaderCommit: rf.commitIndex,//大多数peers认可的日志index
		}
		//初始prev相关为空
		if rf.nextIndex[i] > 0 { //不是初始状态
			args.PrevLogIndex = rf.nextIndex[i] - 1
		}
		//if args.PrevLogIndex > 0 && args.PrevLogIndex > rf.lastLogIndex { //不是初始状态且未被快照过
		if args.PrevLogIndex > 0 { //不是初始状态
			if args.PrevLogIndex == rf.lastLogIndex {
				args.PrevLogTerm = rf.lastLogTerm
			} else {
				args.PrevLogTerm = rf.getLogByIndex(args.PrevLogIndex).Term
			}
		}
		//从何处开始复制日志,如果nextIndex超出已有log,说明无需同步日志,日志为空，即发送心跳
		if rf.nextIndex[i] > 0 && rf.nextIndex[i] <= rf.logLen() {
			args.Entries = rf.cutLogFrom(rf.nextIndex[i])
		}
		
		if len(args.Entries) != 0 {
			LPrintf("Leader%v发送同步日志请求给%v",rf.me,i)
			LPrintf("[附加日志请求为] arg:%+v\n", args)
		} else {
			LPrintf("Leader%v发送心跳给%v",rf.me,i)
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { //Follower回复AE消息
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	LPrintf("[%v收到AE消息]日志长度:%v arg:%+v,----rf.log:%v \n", rf.me,  rf.logLen(), args, rf.log)
	if rf.currentTerm > args.Term { //发来的Leader已经过期了
		reply.Term = rf.currentTerm
		reply.Success = false
		LPrintf("[%v认为Leader过期了]此时%v的任期是%v,发来的Leader%v任期是:%v",rf.me,rf.me,rf.currentTerm,args.LeaderId,args.Term)
		return
	}

	//根据AE消息更新基本信息(心跳相关)
	rf.currentTerm = args.Term
	rf.persist()
	rf.votedmeNum = 0
	rf.role = Follower //Candidate收到AE变成Follower
	rf.overtime = time.Duration(150 + rand.Intn(150)) * time.Millisecond //抑制成为candidate
	rf.timer.Reset(rf.overtime)
	var prevLogTerm int
	if args.PrevLogIndex <= rf.logLen() { //8 < 9
		if args.PrevLogIndex > rf.lastLogIndex {
			prevLogTerm = rf.getLogByIndex(args.PrevLogIndex).Term
		} else if args.PrevLogIndex == rf.lastLogIndex {
			prevLogTerm = rf.lastLogTerm
		} else { // args.PrevLogIndex < rf.lastLogIndex 已经被包含
			reply.Success = true
			return
		}
	}
	//根据AE消息更新日志
	if args.PrevLogIndex > 0 && (args.PrevLogIndex > rf.logLen() || args.PrevLogTerm != prevLogTerm) {
	//有空白槽或前置任期不一致
		reply.Term = rf.currentTerm
		reply.Success = false //不同意附加
		LPrintf("[不同意加日志]%v不同意附加新的日志",rf.me)

		//快速更新相关
		//有空白槽
		if args.PrevLogIndex > rf.logLen() {
			reply.XTerm = -1 //XTerm:冲突的任期号,-1表示为空
			reply.XLen = rf.logLen() + 1 //XLen:如果XTerm=-1出现空白槽时,开始加的位置
			LPrintf("[快速更新]有空白槽, XLen:%v", reply.XLen)
		} else {
		//前置任期不一致
			reply.XTerm = prevLogTerm
			reply.XIndex = args.PrevLogIndex //XIndex:任期号为XTEerm的第一条Log条目的槽位号
			for reply.XIndex > 1 && reply.XIndex - 1 > rf.lastLogIndex && rf.getLogByIndex(reply.XIndex).Term == rf.getLogByIndex(reply.XIndex - 1).Term {
				reply.XIndex-- //找到第一条
			}
			LPrintf("[快速更新]前置日志任期不一致")
		}
		return
	} else { 
	//同意加日志
		if len(args.Entries) != 0 { // 不是心跳
			for index, entry := range args.Entries {
				if entry.Index <= rf.logLen() && rf.getLogByIndex(entry.Index).Term != entry.Term {
					LPrintf("原日志长度:%v", rf.logLen())
					rf.log = rf.cutLogEnd(entry.Index)
					rf.persist()
					LPrintf("截取后日志长度:%v", rf.logLen())
				}
				if entry.Index > rf.logLen() {
					LPrintf("原日志长度:%v", rf.logLen())
					rf.appendLog(args.Entries[index:])
					rf.persist()
					LPrintf("附加后日志长度:%v", rf.logLen())
					break
				}
			}
			rf.commitIndex = min(rf.commitIndex, rf.logLen())
		}
		reply.Term = rf.currentTerm
		
		if len(args.Entries) == 0 {
			LPrintf("%v返回心跳",rf.me)
		} else {
			LPrintf("%v返回AE消息,成功同步日志,同步日志的index:%v,term:%v", rf.me, args.PrevLogIndex+1, args.Entries[0].Term)
		}
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = min(rf.logLen(), args.LeaderCommit)
			rf.apply()
		}
		reply.Success = true
		if len(args.Entries) != 0 {
			LPrintf("[%v成功附加日志] arg:%+v,----rf.logs:%v \n", rf.me, args, rf.log)
		}
	}
	
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		return false
	}
	LPrintf("%v发送AE消息给%v,%v返回结果：%v",rf.me,server,server,reply.Success)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//收到过期的rpc回复
	if rf.role != Leader || args.Term != rf.currentTerm {
		return false
	}

	if reply.Term > rf.currentTerm { //回复的Term更大，Leader变为Follower
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1 //更新投票相关
		rf.persist()
		rf.votedmeNum = 0
		LPrintf("%v由Leader转变为Follower",rf.me)
		return ok
	}

	if reply.Success == true && args.Entries == nil { //心跳
		return ok
	}

	if reply.Success == false {
		LPrintf("%v节点不同意附加日志",server)
		if reply.Term != 0 { //回复不是心跳
			LPrintf("快速更新相关:reply:%+v",reply)
			if reply.XTerm == -1 { //空白槽
				rf.nextIndex[server] = reply.XLen
				LPrintf("快速更新rf.nextIndex[server]:%v",rf.nextIndex[server])
				if(rf.nextIndex[server] > 0) {
					args.PrevLogIndex =  rf.nextIndex[server] - 1
				}
				if rf.nextIndex[server] - 1 <= rf.lastLogIndex {
					return ok
				}
				if(args.PrevLogIndex > 0) {
					args.PrevLogTerm = rf.getLogByIndex(args.PrevLogIndex).Term
				}
				if(rf.nextIndex[server] > 0) {
					args.Entries = rf.cutLogFrom(rf.nextIndex[server])
				}
				LPrintf("快速更新AE消息:%+v",args)
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(server, args, &reply)
			} else { // 前置任期不一致
				LPrintf("test:rf.nextIndex[server]:%v",rf.nextIndex[server])
				if reply.XIndex <= rf.lastLogIndex { // 要同步的已被快照保存
					if reply.XIndex <= rf.lastLogIndex && rf.lastLogIndex != 0{ 
						arg := SnapshotArgs{
							Term:				rf.currentTerm,
							LeaderId:			rf.me,
							LastIncludedIndex:  rf.lastLogIndex,
							LastIncludedTerm:   rf.lastLogTerm,
							Data:				rf.persister.ReadSnapshot(),
						}
						rep := SnapshotReply{}
						go rf.sendInstallSnapshot(server, &arg, &rep) //先同步快照
						return ok
					}
					rf.nextIndex[server] = rf.lastLogIndex + 1
				} else if rf.getLogByIndex(reply.XIndex).Term != reply.Term {
					rf.nextIndex[server] = reply.XIndex
				} else {
					rf.nextIndex[server] = reply.XIndex + 1
				}
				LPrintf("快速更新rf.nextIndex[server]:%v",rf.nextIndex[server])
				if(rf.nextIndex[server] > 0) {
					args.PrevLogIndex =  rf.nextIndex[server] - 1
				}
				if args.PrevLogIndex > 0 {
					CPrintf("test:args.PrevLogIndex:%v, 日志现长:%v, 日志总长:%v", args.PrevLogIndex, len(rf.log), rf.logLen())
					if args.PrevLogIndex == rf.lastLogIndex {
						args.PrevLogTerm = rf.lastLogTerm
					} else {
						args.PrevLogTerm = rf.getLogByIndex(args.PrevLogIndex).Term
					}
				}
				if(rf.nextIndex[server] > 0) {
					args.Entries = rf.cutLogFrom(rf.nextIndex[server])
				}
				LPrintf("快速更新AE消息:%+v",args)
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(server, args, &reply)
			}
		}
	} else { //附加日志成功
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		for n := rf.lastLogIndex + 1; n <= rf.logLen(); n++ {
			if rf.getLogByIndex(n).Term != rf.currentTerm {
				continue
			}
			applyNum := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					applyNum++
				}
				if applyNum > len(rf.peers)/2 {
					rf.commitIndex = n 
					rf.apply()
					break
				}
			}
		}
		return ok
	}
	return ok
}


func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	LPrintf("[%v]: rf.applyCond.Broadcast()", rf.me)
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			LPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
		if rf.lastApplied > rf.logLen() {
			rf.mu.Unlock()
			return
		}
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]LogEntry, commitIndex - lastApplied)
		copy(entries, rf.log[lastApplied - rf.lastLogIndex: commitIndex - rf.lastLogIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			applyMsg := ApplyMsg{
				SnapshotValid: false,
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
			rf.applyChan <- applyMsg
		}
		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}
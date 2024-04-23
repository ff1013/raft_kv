package raft

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// 日志长度
func (rf *Raft) logLen() int {
	return len(rf.log) + rf.lastLogIndex
}

// index对应日志
func (rf *Raft) getLogByIndex(index int) LogEntry{
	DPrintf(rf.me, "[getLogByIndex]:index:%v, rf.lastLogIndex:%v, len(rf.log):%v", index, rf.lastLogIndex, len(rf.log))
	return rf.log[index - rf.lastLogIndex - 1]
	
}

// 最后一条日志Index
func (rf *Raft) getlastLogIndex() int{
	if rf.logLen() > rf.lastLogIndex {
		return rf.log[rf.logLen() - rf.lastLogIndex - 1].Index
	} else if rf.logLen() <= rf.lastLogIndex{
		return rf.lastLogIndex
	}
	return -1
}

// 最后一条日志Term
func (rf *Raft) getlastLogTerm() int{
	if rf.logLen() > rf.lastLogIndex {
		return rf.log[rf.logLen() - rf.lastLogIndex - 1].Term
	} else if rf.logLen() <= rf.lastLogIndex{
		return rf.lastLogTerm
	}
	return -1
}

// 截取从index到最后的日志
func (rf *Raft) cutLogFrom(index int) []LogEntry{
	return append([]LogEntry{}, rf.log[index - rf.lastLogIndex - 1:]...) // 修改为深拷贝
	//return rf.log[index- rf.lastLogIndex - 1:]
}

// 截取从开始到index的日志
func (rf *Raft) cutLogEnd(index int) []LogEntry{
	return append([]LogEntry{}, rf.log[:index - rf.lastLogIndex - 1]...)
	//return rf.log[:index - rf.lastLogIndex - 1]
}

// 追加日志
func (rf *Raft) appendLog(addLog []LogEntry){
	rf.log = append([]LogEntry{}, rf.log...)
	rf.log = append(rf.log, addLog...)
}
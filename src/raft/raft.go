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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raft_kv_backend/gob_check"
	"raft_kv_backend/network"
	"raft_kv_backend/persist"
)

//每个节点的角色
type Role int

//枚举节点的角色：领导/追随者/选举人
const (
	Leader Role = iota
	Follower
	Candidate
)

var HeartBeatTimeout = 50 * time.Millisecond

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
//日志被提交后,向applyCh发送ApplyMsg
type ApplyMsg struct {
	CommandValid bool //true表示ApplyMsg包含最新提交的日志条目,如果要发送snapshot快照设置为false
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool   // 是否启用快照
	Snapshot      []byte // 快照
	SnapshotTerm  int    // 快照保存的最新 term
	SnapshotIndex int    // 快照保存的最新 index
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []network.ClientEnd // RPC end points of all peers 所有peers的RPC端点
	persister persist.Persister   // Object to hold this peer's persisted state 用于保持此对等方的持久化状态的对象
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2B
	applyChan chan ApplyMsg //通过applyChan发送提交日志
	applyCond *sync.Cond

	// 2A
	// 持久化状态存于所有服务器,Persistent state on all servers

	currentTerm int        //当前任期term
	votedFor    int        //给谁投了票
	log         []LogEntry //日志条目数组,log存放着每个LogEntry的Leader任期号Term和指令Command

	// 非持久状态存于所有服务器,Volatile state on all servers

	commitIndex int //已被提交的槽位
	lastApplied int //最后追加到日志中的槽位

	// 非持久状态存于Leader,Volatile state on leaders
	// 用来管理follower

	nextIndex  []int //对于它的Follower,需要从哪里发送来同步日志
	matchIndex []int //对于它的Follower,已经同步复制成功的日志下标

	// 节点状态信息

	role          Role          //Leader/Follower/Candidate
	overtime      time.Duration //超时时间,随机为150~300ms
	timer         *time.Ticker  //选举计时器
	votedmeNum    int           //投票给自己的数量
	applyNum      int           //附加日志成功的peer数量,用来判断是否commit日志
	applyNumIndex []int

	// 最新快照相关
	lastLogIndex int
	lastLogTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { //是否是Leader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// 让kvraft获取当前raft状态的大小
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// 让kvraft获取当前raft的快照
func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

// kvraft获取最新快照下标
func (rf *Raft) GetSnapshotLastIndex() int {
	return rf.lastLogIndex
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistData() []byte {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob_check.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.lastLogTerm)
	data := w.Bytes()
	return data
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
	r := bytes.NewBuffer(data)
	d := gob_check.NewDecoder(r)
	var log []LogEntry
	var currentTerm int
	var votedFor int
	var lastLogIndex int
	var lastLogTerm int
	if d.Decode(&log) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&lastLogIndex) != nil || d.Decode(&lastLogTerm) != nil {
		DPrintf(rf.me, "持久化失败")
	} else {
		DPrintf(rf.me, "持久化恢复")
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastLogIndex = lastLogIndex
		rf.lastLogTerm = lastLogTerm
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
//当前槽位索引index,当前任期,是否是Leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.role != Leader {
		return index, term, false
	}

	//初始化日志条目，进行追加
	appendLog := LogEntry{
		Term:    rf.currentTerm, //任期
		Command: command,        //命令接口
		Index:   rf.logLen() + 1,
	}
	rf.log = append(rf.log, appendLog)
	rf.persist()
	rf.applyNum = 1                      //附加日志成功数量
	for i := 0; i < len(rf.peers); i++ { //更新nextIndex
		if i == rf.me {
			continue
		}
	}
	index = rf.logLen()
	term = rf.currentTerm
	LPrintf(rf.me, "%v追加条目index:%v,term:%v", rf.me, index, term)
	// 收到客户端command后立即触发appendEntries()逻辑
	rf.appendEntries()
	return index, term, isLeader
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
func (rf *Raft) ticker() { // 开始选举/附加日志
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			switch rf.role {
			case Follower: //追随者
				rf.role = Candidate //时间到了变成选举者
				fallthrough
			case Candidate: //选举者
				rf.leaderElection()
			case Leader:
				rf.appendEntries()
			}
			rf.mu.Unlock()
		}
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
func Make(peers []network.ClientEnd, me int,
	persister persist.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//2B
	rf.applyChan = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	//2A
	//Persistent state on all servers
	rf.currentTerm = 0           //初始为0
	rf.votedFor = -1             //最开始没有投票
	rf.log = make([]LogEntry, 0) //起始没有日志

	//Volatile state on all servers
	rf.commitIndex = 0 //初始没有日志
	rf.lastApplied = 0 //初始没有日志

	//Volatile state on leaders
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	//节点状态信息
	rf.role = Follower                                                 //初始为Follower
	rf.overtime = time.Duration(150+rand.Intn(150)) * time.Millisecond //随机产生150~300ms
	rf.timer = time.NewTicker(rf.overtime)
	rf.votedmeNum = 0 //给自己投票的数量
	rf.applyNum = 0   //附加日志成功数量
	rf.applyNumIndex = make([]int, len(peers))

	// 最新快照相关
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 根据持久化数据更新
	rf.lastApplied = rf.lastLogIndex

	// start ticker goroutine to start elections
	//开始选举
	go rf.ticker()
	go rf.applier()

	return rf
}

package raft

import (
	"math/rand"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct { //请求投票
	// Your data here (2A, 2B).
	Term          int //自己(竞选者)的任期
	CandidateId   int //自己(竞选者)的Id
	//其他人通过下面这两项来判断要不要投票
	LastLogIndex  int //自己(竞选者)的日志条目最后槽位
	LastLogTerm   int //自己(竞选者)的最后日志条目任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct { //答复投票结果
	// Your data here (2A).
	Term          int  //自己(投票者)的任期,用于判断是否投票
	VoteGranted   bool //是否同意投票,true是投票
}

func (rf *Raft) leaderElection() {
	//初始化自己的任期,投票给自己
	rf.currentTerm += 1
	SPrintf("%v成为候选人，给自己投票,自己的任期是%v,自己的日志:%+v", rf.me, rf.currentTerm, rf.log)
	rf.votedFor =rf.me //给自己投票
	rf.persist()
	rf.votedmeNum = 1 //自己的投票数初始为1
	//每轮选举都要重设选举超时
	rf.overtime = time.Duration(150 + rand.Intn(150)) * time.Millisecond // 随机产生150-300ms
	rf.timer.Reset(rf.overtime)

	//对自身以外的节点进行选举投票请求
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me { //除自身以外
			continue
		}
		//RPC参数设置
		voteArgs := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.logLen(),
			LastLogTerm:  0,
		}
		if rf.logLen() > 0 && voteArgs.LastLogIndex > rf.lastLogIndex { //最后日志槽位对应的term
			voteArgs.LastLogTerm = rf.getLogByIndex(voteArgs.LastLogIndex).Term
		} else {
			voteArgs.LastLogTerm = rf.lastLogTerm
		}
		voteReply := RequestVoteReply{}
		SPrintf("给%v发送投票请求",i)
		go rf.sendRequestVote(i, &voteArgs, &voteReply)
	}
}


//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	SPrintf("节点 %v 开始判断要不要投票给 %v, 自己的log: %+v ",rf.me, args.CandidateId, rf.log)
	reply.Term = rf.currentTerm //自己(投票者)的任期,用于判断是否投票
	reply.VoteGranted = false //是否同意投票,true是投票

	//任期Term相等时,已经投过票且投的票不是此竞选者
	//任期Term不相等时，不符合if条件，可以继续向下执行，重新投票给更高Term的竞选者
	if args.Term == rf.currentTerm &&rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		SPrintf("节点已经投过票了,投给了: %v",rf.votedFor)
		return //不投票
	}

	if args.Term < rf.currentTerm { //请求者任期小于投票者任期
		SPrintf("请求者任期小于投票者任期")
		return //不投票
	}

	if args.Term >= rf.currentTerm { //请求者任期大于投票者任期
		rf.role = Follower //更新状态为follower
		rf.currentTerm = args.Term //更新Term
		rf.persist()

		//进一步判断是否投票
		lastLogTerm := 0 //投票者自己的日志最后任期
		if rf.logLen() !=0 && rf.logLen() >= rf.lastLogIndex {
			lastLogTerm = rf.getlastLogTerm()
		}

		//比较投票者日志最后任期和竞选者日志最后任期
		if lastLogTerm > args.LastLogTerm { //竞选者日志最后任期小于投票者日志最后任期
			SPrintf("竞选者日志最后任期:%v小于投票者日志最后任期:%v", args.LastLogTerm, lastLogTerm)
			rf.votedFor = -1
			rf.persist()
			return //不投票
		} else if lastLogTerm == args.LastLogTerm && args.LastLogIndex < rf.logLen() { //等于，但是日志长度短
			SPrintf("日志最后日期相同但是日志长度短,%v,%v",args.LastLogIndex, rf.logLen())
			return //不投票
		}

		//投票
		rf.votedFor = args.CandidateId
		SPrintf("[投票者]%v同意投票给%v",rf.me,args.CandidateId)
		rf.currentTerm = args.Term
		rf.persist()
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.overtime = time.Duration(150 + rand.Intn(150)) * time.Millisecond //随机产生150~300ms
		rf.timer.Reset(rf.overtime)
		return
	}

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
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//收到过期的rpc回复
	if rf.role != Candidate || args.Term != rf.currentTerm {
		return false
	}

	if reply.Term > rf.currentTerm { //回复的Term更大，Candicate变为Follower
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1 //更新投票相关
		rf.persist()
		rf.votedmeNum = 0
		SPrintf("%v由Candicate转变为Follower",rf.me)
	}

	if reply.VoteGranted { //回复为同意投票，投票数++
		SPrintf("[候选人收到]%v同意投票给%v",server,rf.me)
		rf.votedmeNum++;
	}

	if rf.votedmeNum > len(rf.peers)/2 && rf.role == Candidate {
		rf.role = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.logLen() + 1
			rf.matchIndex[i] = 0 // 每次重新上任，需要将rf.matchIndx归零
		}
		rf.timer.Reset(HeartBeatTimeout)
		SPrintf("%v成为Leader",rf.me)
	}
	return ok
}


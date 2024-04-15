package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"raft_kv_backend/gob_check"
	"raft_kv_backend/network"
	"raft_kv_backend/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct { // 接收上层client参数，发往下层raft
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
	OpString  string
	Key       string
	Value     string
}

// raft层返回的内容
type Response struct {
	Err   Err
	Value string
}

// 请求的相关信息
type CommandContext struct {
	CommandId    int64    // 每个请求有唯一id
	CommandReply Response // 保存上一次回复
}

// 唯一标识日志
type IndexTerm struct {
	Index int
	Term  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 3个map:1个存储数据,1个记录Client最新Command的id,1个用于raft层apply后通知rpc
	kvMap     map[string]string           // (key, value)
	cliComMap map[int64]CommandContext    // (clientId, commandId+commandReply)
	applyMap  map[IndexTerm]chan Response //(index+term, chan)

	lastApplied       int // 记录最后apply的日志index，防止过期快照使得日志过期，以及已保存在快照的日志apply
	lastSnapshotIndex int // 上次快照的commit index
}

// 获取与raft层交互的response的channel
func (kv *KVServer) getApplyReplyChan(index int, term int) chan Response {
	ch, ok := kv.applyMap[IndexTerm{index, term}]
	if !ok { // 不存在,初始化创建一个
		//DPrintf("[创建一个channel]index:%v, term:%v", index, term)
		kv.applyMap[IndexTerm{index, term}] = make(chan Response, 1)
		ch = kv.applyMap[IndexTerm{index, term}]
	}
	return ch
}

// get操作
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[收到Get请求]server:%v请求key:%v", kv.me, args.Key)
	// Your code here.
	// 发往raft参数设置
	getOp := Op{
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		OpString:  "Get",
		Key:       args.Key,
	}

	// 调用和下层raft交互函数
	response := kv.serverRaftProcess(getOp)
	//DPrintf("调用下层函数")
	reply.Err, reply.Value = response.Err, response.Value
	DPrintf("raft回复get请求结果:%v", reply.Value)
}

// put/append操作
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 发往raft参数设置
	DPrintf("[收到%v请求]server:%v请求key:%v, value:%v, commandId:%v", args.Op, kv.me, args.Key, args.Value, args.CommandId)
	putAppendOp := Op{
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		OpString:  args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}

	// 调用和下层raft交互函数
	response := kv.serverRaftProcess(putAppendOp)
	reply.Err = response.Err
	DPrintf("raft回复%v请求结果:%v", args.Op, reply.Err)
}

// 请求是否重复
func (kv *KVServer) isRepeat(op *Op) bool {
	if command, ok := kv.cliComMap[op.ClientId]; ok {
		if command.CommandId >= op.CommandId {
			DPrintf("%v的%v请求重复发送测试:op.CommandId:%v, command:%v", kv.me, op, op.CommandId, command)
			//若当前的请求已经被执行过了
			return true
		}
	}
	return false
}

// server与raft交互,处理client发来的请求get/put/append
// 参数Op发往raft,参数Response从raft接收
func (kv *KVServer) serverRaftProcess(commandOp Op) Response {
	var response Response
	// 如果请求是put/append，需检查请求是否重复发送，来保证线性一致性
	kv.mu.Lock()
	if commandOp.OpString != "Get" && kv.isRepeat(&commandOp) {
		kv.mu.Unlock()
		response.Err = "OK"
		response.Value = ""
		DPrintf("put/append请求重复发送")
		return response
	}
	kv.mu.Unlock()

	// 调用raft的Start函数
	//DPrintf("[加入新日志]调用raft的Start函数")
	index, term, isLeader := kv.rf.Start(commandOp)

	// 不是Leader
	if !isLeader {
		//DPrintf("[失败]:%v不是Leader", kv.me)
		response.Err = ErrWrongLeader
		return response
	}
	//DPrintf("Leader")

	// 获取reply的channel
	kv.mu.Lock()
	replyCh := kv.getApplyReplyChan(index, term)
	kv.mu.Unlock()
	//DPrintf("[replyCh]获取和raft交互的replyCh")

	defer func() {
		kv.mu.Lock()
		delete(kv.applyMap, IndexTerm{index, term})
		kv.mu.Unlock()
		// close(replyCh)
	}()

	// 监听，1、applyCh及时获取回复，2、请求超时
	select {
	case replyMsg := <-replyCh:
		//if response.
		response.Err, response.Value = replyMsg.Err, replyMsg.Value
		DPrintf("[收到raft写入日志回复]%v", response.Err)
		return response
	case <-time.After(100 * time.Millisecond):
		DPrintf("[失败]:%v处理%v请求超时", kv.me, commandOp)
		response.Err = ErrTimeout
		return response
	}
}

// 从applyCh中接收ApplyMsg
func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			// DPrintf("[raft进行apply]:%v", applyMsg.CommandIndex)
			if applyMsg.CommandValid { // 日志压缩关闭
				//DPrintf("[日志压缩关闭]")
				kv.mu.Lock()
				DPrintf("[检查msg过期]applyMsg.CommandIndex:%v,kv.lastApplied:%v", applyMsg.CommandIndex, kv.lastApplied)
				if applyMsg.CommandIndex <= kv.lastApplied { // 比lastApplied还小，说明msg过期
					DPrintf("[apply失败]过期msg, msg的index:%v, lastApplied:%v", applyMsg.CommandIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				// 接收msg后更新lastApplied
				kv.lastApplied = applyMsg.CommandIndex
				// DPrintf("[更新lastApplied]%v的lastApplied:%v", kv.me, kv.lastApplied)

				var response Response
				op := applyMsg.Command.(Op)                   // 将command转换成Op格式
				if op.OpString != "Get" && kv.isRepeat(&op) { // put/append检查是否重复请求
					DPrintf("put/append请求重复发送")
					response = kv.cliComMap[op.ClientId].CommandReply // 上次回复结果
				} else {
					// kv.cliComMap[op.ClientId] = op.CommandId // 更新ClientId[CommandId]的Map
					switch op.OpString {
					case "Put":
						// DPrintf("test,%v",op.OpString)
						kv.kvMap[op.Key] = op.Value
						response = Response{OK, ""}
						DPrintf("[Put请求成功]server:%v, key:%v, value:%v, kvMap:%v", kv.me, op.Key, op.Value, kv.kvMap)
					case "Append":
						kv.kvMap[op.Key] += op.Value
						response = Response{OK, ""}
						DPrintf("[Append请求成功]server:%v, key:%v, value:%v, kvMap:%v", kv.me, op.Key, kv.kvMap[op.Key], kv.kvMap)
					case "Get":
						// 遍历检查key是否存在
						if value, ok := kv.kvMap[op.Key]; ok {
							DPrintf("[Get请求成功]server:%v, key:%v, value:%v, kvMap:%v", kv.me, op.Key, value, kv.kvMap)
							response = Response{OK, value}
						} else {
							DPrintf("[Get请求失败]server:%v, key:%v不存在, kvMap:%v", kv.me, op.Key, kv.kvMap)
							response = Response{ErrNoKey, ""}
						}
					}
					if op.OpString != "Get" {
						kv.cliComMap[op.ClientId] = CommandContext{op.CommandId, response} // 更新ClientId[CommandId]的Map
					}
				}
				// raft保存超出最大限制长度，保存快照
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					kv.raftSnapshot(applyMsg.CommandIndex)
				}
				// raft apply的结果返回给raft
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == applyMsg.CommandTerm {
					replyCh := kv.getApplyReplyChan(applyMsg.CommandIndex, currentTerm)
					replyCh <- response
					DPrintf("结果返回给raft")
				}
				kv.mu.Unlock()
			} else if applyMsg.SnapshotValid { // 日志压缩开启
				// 确保快照不发生冲突，实际上由于代码编写一定为true
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					// 收到raft进行apply的快照，根据index是否需要安装
					if applyMsg.SnapshotIndex > kv.lastApplied {
						kv.lastApplied = applyMsg.SnapshotIndex
						DPrintf("保存快照1")
						kv.raftInstallSnapshot(applyMsg.Snapshot) // 安装快照
					}
				}
				kv.mu.Unlock()
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []network.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call gob_check.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob_check.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.cliComMap = make(map[int64]CommandContext)
	kv.applyMap = make(map[IndexTerm]chan Response)

	kv.lastApplied = -1

	// crash后恢复快照

	kv.raftInstallSnapshot(kv.rf.GetSnapshot())

	go kv.applier()
	//go kv. generateSnapshotTask()

	DPrintf("[创建server成功] %v", kv.me)
	DPrintf("kv.lastApplied:%v", kv.lastApplied)

	return kv
}

// 调用raft保存快照
func (kv *KVServer) raftSnapshot(snapshotIndex int) {
	DPrintf("%v保存快照, kvMap:%v, cliComMap:%v", kv.me, kv.kvMap, kv.cliComMap)
	w := new(bytes.Buffer)
	e := gob_check.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.cliComMap)
	e.Encode(kv.lastApplied)
	data := w.Bytes()
	kv.rf.Snapshot(snapshotIndex, data)
	kv.lastSnapshotIndex = snapshotIndex
}

// 调用raft安装快照
func (kv *KVServer) raftInstallSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := gob_check.NewDecoder(r)
	var kvMap map[string]string
	var cliComMap map[int64]CommandContext
	var lastApplied int

	if d.Decode(&kvMap) != nil || d.Decode(&cliComMap) != nil || d.Decode(&lastApplied) != nil {
		DPrintf("安装快照失败")
	} else {
		kv.kvMap = kvMap
		kv.cliComMap = cliComMap
		DPrintf("%v安装快照成功,kvMap:%v, rf.snaplastIndex:%v, cliComMap:%v", kv.me, kv.kvMap, kv.rf.GetSnapshotLastIndex(), kv.cliComMap)
		kv.lastApplied = lastApplied
	}
}

// 定期检查快照
func (kv *KVServer) generateSnapshotTask() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate && kv.lastApplied > kv.lastSnapshotIndex {
			DPrintf("保存快照2")
			kv.raftSnapshot(kv.lastApplied)
			kv.lastSnapshotIndex = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

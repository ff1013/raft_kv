package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"raft_kv_backend/network"
)


type Clerk struct {
	servers []network.ClientEnd // 所有服务器
	// You will have to modify this struct.
	mu sync.Mutex // 锁
	leaderId int // Leader的id
	clientId int64 // 当前客户端的id,注意为int64
	lastAppliedCommandId int64 // 上次Apply了的Command的id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []network.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0 // 初始不知道谁是Leader
	ck.clientId = nrand()
	ck.lastAppliedCommandId = 0 // 初始
	DPrintf("[创建客户端成功] %v", ck.clientId)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// 获取当前key的value，key不存在返回""空字符串
// 碰到其他错误则一直尝试下去
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	serverNum := len(ck.servers) // 服务器数量
	ck.lastAppliedCommandId++
	args := GetArgs {
		Key:       key,
		ClientId:  ck.clientId,
		CommandId: ck.lastAppliedCommandId,
	}
	DPrintf("[客户端调用请求]%v请求Get, key:%v commandId:%v", ck.clientId, key, args.CommandId)
	for serverId := ck.leaderId ; ; serverId = (serverId + 1) % serverNum {
		reply := GetReply {}
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		// 发送失败
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout{
			// DPrintf("[请求发送失败]")
			continue
		}
		// 发送成功
		DPrintf("[请求发送成功]")
		ck.leaderId = serverId
		ck.lastAppliedCommandId = args.CommandId
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	serverNum := len(ck.servers) // 服务器数量
	ck.lastAppliedCommandId++
	args := PutAppendArgs {
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		CommandId: ck.lastAppliedCommandId,
	}
	DPrintf("[客户端调用请求]%v请求%v, key:%v, value:%v, commandId:%v", ck.clientId, args.Op, key, value, args.CommandId)
	for serverId := ck.leaderId ; ; serverId = (serverId + 1) % serverNum {
		reply := PutAppendReply {}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		// 发送失败
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			continue
		}
		// 发送成功
		ck.leaderId = serverId
		ck.lastAppliedCommandId++
		return "ok"
	}
}

func (ck *Clerk) Put(key string, value string) string {
	// DPrintf("[客户端调用请求]%v请求Put, key:%v, value:%v", ck.clientId, key, value)
	return ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) string {
	// DPrintf("[客户端调用请求]%v请求Append, key:%v, value:%v", ck.clientId, key, value)
	return ck.PutAppend(key, value, "Append")
}

// 客户端Delete请求
func (ck *Clerk) Delete(key string) string {
	serverNum := len(ck.servers) // 服务器数量
	ck.lastAppliedCommandId++
	args := DeleteArgs {
		Key:       key,
		ClientId:  ck.clientId,
		CommandId: ck.lastAppliedCommandId,
	}
	DPrintf("[客户端调用请求]%v请求Delete, key:%v commandId:%v", ck.clientId, key, args.CommandId)
	for serverId := ck.leaderId ; ; serverId = (serverId + 1) % serverNum {
		reply := DeleteReply {}
		ok := ck.servers[serverId].Call("KVServer.Delete", &args, &reply)
		// 发送失败
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout{
			// DPrintf("[请求发送失败]")
			continue
		}
		// 发送成功
		DPrintf("[请求发送成功]")
		ck.leaderId = serverId
		ck.lastAppliedCommandId = args.CommandId
		if reply.Err == ErrNoKey {
			return ""
		}
		return "ok"
	}
}
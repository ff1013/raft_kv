package main

import (
	"raft_kv_backend/kvraft"
	// "raft_kv_backend/raft"
	"raft_kv_backend/network"
	"raft_kv_backend/persist"
	"flag"
	"net/http"
	"net/rpc"
	"strconv"
)

func main() {
	flag.Parse()
	args := flag.Args()
	var servers []network.ClientEnd
	for i := 0; i < len(args)-1; i++ {
		servers = append(servers, network.MakeNetClientEnd(args[i]))
	}
	me, _ := strconv.Atoi(args[len(args)-1])
	me--

	// var saved *raft.Persister

	// saved = raft.MakePersister()

	// 默认触发快照大小为 512M 后期改成可配置
	maxsaftstate := 1024 * 1024 * 512
	server := kvraft.StartKVServer(servers, me, persist.MakeFilePersister(me), maxsaftstate) // 使用levelDB
	// server := kvraft.StartKVServer(servers, me, saved, maxsaftstate)
	rpc.Register(server)
	rpc.Register(server.Rf)
	rpc.HandleHTTP()
	http.ListenAndServe(args[me], nil)
}

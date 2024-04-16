package main

import (
	"raft_kv_backend/kvraft"
	"raft_kv_backend/network"
	"flag"
	"fmt"
)

func main() {
	flag.Parse()
	args := flag.Args()
	var servers []network.ClientEnd
	for i := range args {
		servers = append(servers, network.MakeNetClientEnd(args[i]))
	}

	client := kvraft.MakeClerk(servers)

	for true {
		fmt.Print("> ")
		var op string
		var key string
		var value string
		fmt.Scanf("%s %s", &op, &key)
		if op != "Get" {
			fmt.Scanf("%s", &value)
		}

		// 清除输入缓冲区中的换行符
		fmt.Scanln()

		var res string
		switch op {
		case "Get":
			res = client.Get(key)
		case "Put":
			res = client.Put(key, value)
		case "Append":
			res = client.Append(key, value)
		}
		fmt.Println(res)
	}
}

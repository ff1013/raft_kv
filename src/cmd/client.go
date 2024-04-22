package main

import (
	"flag"
	"fmt"
	"net/http"
	"raft_kv_backend/kvraft"
	"raft_kv_backend/network"
	"raft_kv_backend/http"

	"github.com/rs/cors"
)

func main() {
	flag.Parse()
	args := flag.Args()
	var servers []network.ClientEnd
	for i := range args {
		servers = append(servers, network.MakeNetClientEnd(args[i]))
	}

	client := kvraft.MakeClerk(servers)

	// 创建一个带有 CORS 中间件的处理程序
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowCredentials: true,
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Content-Type"},
	})
	httpServer := &http.Server{
		Addr: "127.0.0.1:12100",
		Handler: c.Handler(&raft_kv_http.KvServer{
			Client: client,
		}),
	}
	go httpServer.ListenAndServe()
	
	fmt.Print("function: Get、Put、Append、Delete\n")
	for true {
		fmt.Print("> ")
		var op string
		var key string
		var value string
		fmt.Scanf("%s %s", &op, &key)
		if op == "Put" || op == "Append" {
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
		case "Delete":
			res = client.Delete(key)
		default:
			res = "There is no such operation, currently supported operations: Get、Put、Append、Delete"
		}
		fmt.Println(res)
	}
}

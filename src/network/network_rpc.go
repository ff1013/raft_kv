package network

import (
	"net/rpc"
)

type NetClientEnd struct {
	ipAddr string
}

func MakeNetClientEnd(ipAddr string) *NetClientEnd {
	return &NetClientEnd{
		ipAddr,
	}
}

func (n *NetClientEnd) Call(rpcName string, args interface{}, reply interface{}) bool {
	rpc, _ := rpc.DialHTTP("tcp", n.ipAddr)
	defer func() {
		_ = recover()
	}()
	defer rpc.Close()
	rpc.Call(rpcName, args, reply)
	return true
}
package network

type ClientEnd interface {
	// Call 调用rpcName，参数args，结果reply，返回值bool表示请求是否成功
	Call(rpcName string, args interface{}, reply interface{}) bool
}
package raft

import "log"

// Debugging

const all = true // 一键关闭日志

const Debug = true && all // 持久化
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const SDebug = true && all // 选举相关日志
func SPrintf(format string, a ...interface{}) (n int, err error) {
	if SDebug {
		log.Printf(format, a...)
	}
	return
}

const LDebug = true && all // 日志附加相关日志
func LPrintf(format string, a ...interface{}) (n int, err error) {
	if LDebug {
		log.Printf(format, a...)
	}
	return
}

const CDebug = true && all // 日志压缩相关日志
func CPrintf(format string, a ...interface{}) (n int, err error) {
	if CDebug {
		log.Printf(format, a...)
	}
	return
}

// go中没有int的max，min函数，自己新增
func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
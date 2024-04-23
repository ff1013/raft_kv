package raft

import (
	"fmt"
	"log"
	"os"
	"sync"
)

// Debugging

var (
	logFile *os.File
	logger *log.Logger
	once sync.Once
	err error
)

func initLogger(id int) {
	once.Do(func() {
		// 输出日志到文件中
		logFileName := fmt.Sprintf("raft_%d.log", id)
		logFile, err = os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("打开日志文件失败：%v", err)
		}
		// 创建一个新的log.Logger实例
		logger = log.New(logFile, "", log.LstdFlags)
	})
}

const all = true // 一键关闭日志
const Debug = true && all // 持久化
func DPrintf(id int, format string, a ...interface{}) (n int, err error) {
	if Debug {
		initLogger(id)
		logger.Printf(format, a...)
	}
	return
}

const SDebug = true && all// 选举相关日志
func SPrintf(id int, format string, a ...interface{}) (n int, err error) {
	if SDebug {
		initLogger(id)
		logger.Printf(format, a...)
	}
	return
}


const LDebug = true && all// 日志附加相关日志
func LPrintf(id int, format string, a ...interface{}) (n int, err error) {
	if LDebug {
		initLogger(id)
		logger.Printf(format, a...)
	}
	return
}

const CDebug = true && all // 日志压缩相关日志
func CPrintf(id int, format string, a ...interface{}) (n int, err error) {
	if CDebug {
		initLogger(id)
		logger.Printf(format, a...)
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
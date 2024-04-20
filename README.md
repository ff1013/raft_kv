raft_kv_backend

一、代码运行说明
1、编译后终端运行
（1）编译
    客户端编译：
        go build -o raft-kv-client.exe client.go
    服务器编译：
        go build -o raft-kv-server.exe server.go
（2）终端运行
    服务器运行：
        ./raft-kv-server 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 1
        ./raft-kv-server 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 2
        ./raft-kv-server 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 3
    客户端运行：
        ./raft-kv-client 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002
2、直接终端运行
服务器运行：
    go run server.go 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 1
    go run server.go 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 2
    go run server.go 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 3
客户端运行：
    go run client.go 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002

二、代码分支说明
1、main：最新代码分支
2、raft：保留原lab的raft部分代码(lab2)
3、real_network：raft的rpc通信部分是模拟的，本分支改为使用真实的rpc通信
4、raft_kv：增加kv存储层与raft层之间接口实现(lab3)
5、levelDB：存储层改为使用levelDB，与代码进行适配，并增加在终端下的客户端、服务器构建代码

三、代码说明
1、cmd：在终端下的客户端、服务器构建代码
2、gob_check：rpc通信是否大写的检查
3、kvraft：kv层raft层之间的接口
4、labrpc：模拟网络环境及模拟rpc
5、leveldb_persist、persist：leveldb的kv持久化存储
6、network：真实rpc通信
7、raft：raft层代码
raft_kv_backend

客户端编译：
     go build -o raft-kv-client.exe client.go

服务器编译：
    go build -o raft-kv-server.exe server.go

服务器运行：
    ./raft-kv-server 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 1
    ./raft-kv-server 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 2
    ./raft-kv-server 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002 3

客户端运行：
    ./raft-kv-client 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002

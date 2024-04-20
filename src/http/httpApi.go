package raft_kv_http

import (
	"raft_kv_backend/kvraft"
	"io"
	"net/http"
)

type KvServer struct {
	Client *kvraft.Clerk
}

func (kv *KvServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer r.Body.Close()
	switch r.Method {
	case http.MethodPost:
		v, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "[kvserver]Failed on POST", http.StatusBadRequest)
			return
		}
		kv.Client.Put(key, string(v))
	case http.MethodPut:
		v, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "[kvserver]Failed on PUT", http.StatusBadRequest)
			return
		}
		kv.Client.Append(key, string(v))
	case http.MethodGet:
		res := kv.Client.Get(key)
		w.Write([]byte(res))
	// 暂无删除
	// case http.MethodDelete:
	// 	kv.Client.Delete(key)
	default:
		w.Header().Set("Allow", http.MethodPost)
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Set("Allow", http.MethodGet)
		// w.Header().Set("Allow", http.MethodDelete)
		http.Error(w, "[kvserver]Method nod Allowed", http.StatusMethodNotAllowed)
	}
}
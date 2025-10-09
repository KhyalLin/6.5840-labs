package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Record struct {
	Version rpc.Tversion
	Value   string
}

type KVServer struct {
	mu sync.Mutex
	data map[string]Record
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]Record),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	record, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		DPrintf("Get|ErrNoKey|key=%s", args.Key)
		return
	}

	reply.Value = record.Value
	reply.Version = record.Version
	reply.Err = rpc.OK
	DPrintf("Get|OK|key=%s|value=%s|version=%d", args.Key, record.Value, record.Version)
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	record, ok := kv.data[args.Key]
	if !ok && args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		DPrintf("Put|ErrNoKey|key=%s|version=%d", args.Key, args.Version)
		return
	}
	if ok && args.Version != record.Version {
		reply.Err = rpc.ErrVersion
		DPrintf("Put|ErrVersion|key=%s|want_version=%d|given_version=%d", args.Key, record.Version, args.Version)
		return
	}

	record.Value = args.Value
	record.Version++
	kv.data[args.Key] = record

	reply.Err = rpc.OK
	DPrintf("Put|OK|key=%s|value=%s|version=%d", args.Key, args.Value, record.Version)
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}

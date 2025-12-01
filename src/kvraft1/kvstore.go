package kvraft

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
)

type Record struct {
	Version rpc.Tversion
	Value   string
}

type KVStore struct {
	mu   sync.Mutex
	data map[string]Record
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]Record),
	}
}

var _ rsm.StateMachine = (*KVStore)(nil)

func (kv *KVStore) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req.(type) {
	case *rpc.GetArgs, rpc.GetArgs:
		return kv.doGet(req)
	case *rpc.PutArgs, rpc.PutArgs:
		return kv.doPut(req)
	}
	return nil
}

func (kv *KVStore) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(kv.data); err != nil {
		log.Fatalf("Snapshot|Failed|err=%v", err)
	}
	return buf.Bytes()
}

func (kv *KVStore) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&kv.data); err != nil {
		log.Fatalf("Restore|Failed|err=%v", err)
	}
}

func (kv *KVStore) doGet(req any) any {
	var args *rpc.GetArgs
	if p, ok := req.(*rpc.GetArgs); ok {
		args = p
	} else {
		val := req.(rpc.GetArgs)
		args = &val
	}
	value, version, err := kv.get(args.Key)
	return &rpc.GetReply{
		Value:   value,
		Version: version,
		Err:     err,
	}
}

func (kv *KVStore) doPut(req any) any {
	var args *rpc.PutArgs
	if p, ok := req.(*rpc.PutArgs); ok {
		args = p
	} else {
		val := req.(rpc.PutArgs)
		args = &val
	}
	err := kv.put(args.Key, args.Value, args.Version)
	return &rpc.PutReply{Err: err}
}

func (kv *KVStore) get(key string) (string, rpc.Tversion, rpc.Err) {
	record, ok := kv.data[key]
	if !ok {
		log.Printf("Get|ErrNoKey|key=%s", key)
		return "", 0, rpc.ErrNoKey
	}

	log.Printf("Get|OK|key=%s|value=%s|version=%d", key, record.Value, record.Version)
	return record.Value, record.Version, rpc.OK
}

func (kv *KVStore) put(key string, value string, version rpc.Tversion) rpc.Err {
	record, ok := kv.data[key]
	if !ok && version != 0 {
		log.Printf("Put|ErrNoKey|key=%s|version=%d", key, version)
		return rpc.ErrNoKey
	}
	if ok && version != record.Version {
		log.Printf("Put|ErrVersion|key=%s|want_version=%d|given_version=%d", key, record.Version, version)
		return rpc.ErrVersion
	}

	record.Value = value
	record.Version++
	kv.data[key] = record

	log.Printf("Put|OK|key=%s|value=%s|version=%d", key, value, record.Version)
	return rpc.OK
}

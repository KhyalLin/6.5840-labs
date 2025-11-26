package kvraft

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"

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

func (kv *KVStore) Get(key string) (string, rpc.Tversion, rpc.Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	record, ok := kv.data[key]
	if !ok {
		log.Printf("Get|ErrNoKey|key=%s", key)
		return "", 0, rpc.ErrNoKey
	}

	log.Printf("Get|OK|key=%s|value=%s|version=%d", key, record.Value, record.Version)
	return record.Value, record.Version, rpc.OK
}

func (kv *KVStore) Put(key string, value string, version rpc.Tversion) rpc.Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()

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

func (kv *KVStore) Encode() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(kv.data); err != nil {
		log.Fatalf("Encode|Failed|err=%v", err)
	}
	return buf.Bytes()
}

func (kv *KVStore) Decode(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&kv.data); err != nil {
		log.Fatalf("Decode|Failed|err=%v", err)
	}
}

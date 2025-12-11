package shardgrp

import (
	"bytes"
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	tester "6.5840/tester1"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

type Record struct {
	Version rpc.Tversion
	Value   string
}

type ShardMemoryKV struct {
	gid         tester.Tgid
	me          int
	data        []map[string]Record
	shardNums   []shardcfg.Tnum
	shardExts   []bool
}

func NewShardMemoryKV(gid tester.Tgid, me int) *ShardMemoryKV {
	kv := &ShardMemoryKV{
		gid:         gid,
		me:          me,
		data:        make([]map[string]Record, shardcfg.NShards),
		shardNums:   make([]shardcfg.Tnum, shardcfg.NShards),
		shardExts:   make([]bool, shardcfg.NShards),
	}
	for i := 0; i < shardcfg.NShards; i++ {
		kv.data[i] = make(map[string]Record)
		kv.shardNums[i] = shardcfg.NumFirst
		kv.shardExts[i] = (gid == shardcfg.Gid1)
	}
	return kv
}

func (kv *ShardMemoryKV) DoOp(req any) any {
	switch req.(type) {
	case *rpc.GetArgs, rpc.GetArgs:
		return kv.doGet(req)
	case *rpc.PutArgs, rpc.PutArgs:
		return kv.doPut(req)
	case *shardrpc.FreezeShardArgs, shardrpc.FreezeShardArgs:
		return kv.doFreezeShard(req)
	case *shardrpc.InstallShardArgs, shardrpc.InstallShardArgs:
		return kv.doInstallShard(req)
	case *shardrpc.DeleteShardArgs, shardrpc.DeleteShardArgs:
		return kv.doDeleteShard(req)
	}
	log.Fatalf("ShardMemoryKV%d-%d.DoOp()|UnknownOp|req=%v", kv.gid, kv.me, req)
	return nil
}

func (kv *ShardMemoryKV) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.shardNums)
	e.Encode(kv.shardExts)
	log.Printf("ShardMemoryKV%d-%d.Snapshot()|OK", kv.gid, kv.me)
	return w.Bytes()
}

func (kv *ShardMemoryKV) Restore(buffer []byte) {
	r := bytes.NewBuffer(buffer)
	d := labgob.NewDecoder(r)
	var data        []map[string]Record
	var shardNums   []shardcfg.Tnum
	var shardExts   []bool
	if d.Decode(&data) != nil ||
		d.Decode(&shardNums) != nil ||
		d.Decode(&shardExts) != nil {
		log.Fatalf("ShardMemoryKV%d-%d.Restore()|Fail", kv.gid, kv.me)
		return
	}

	kv.data = data
	kv.shardNums = shardNums
	kv.shardExts = shardExts
	log.Printf("ShardMemoryKV%d-%d.Restore()|OK", kv.gid, kv.me)
}

func (kv *ShardMemoryKV) doGet(req any) any {
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

func (kv *ShardMemoryKV) doPut(req any) any {
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

func (kv *ShardMemoryKV) doFreezeShard(req any) any {
	var args *shardrpc.FreezeShardArgs
	if p, ok := req.(*shardrpc.FreezeShardArgs); ok {
		args = p
	} else {
		val := req.(shardrpc.FreezeShardArgs)
		args = &val
	}
	state, num, err := kv.freezeShard(args.Shard, args.Num)
	return &shardrpc.FreezeShardReply{
		State: state,
		Num:   num,
		Err:   err,
	}
}

func (kv *ShardMemoryKV) doInstallShard(req any) any {
	var args *shardrpc.InstallShardArgs
	if p, ok := req.(*shardrpc.InstallShardArgs); ok {
		args = p
	} else {
		val := req.(shardrpc.InstallShardArgs)
		args = &val
	}
	err := kv.installShard(args.Shard, args.State, args.Num)
	return &shardrpc.InstallShardReply{Err: err}
}

func (kv *ShardMemoryKV) doDeleteShard(req any) any {
	var args *shardrpc.DeleteShardArgs
	if p, ok := req.(*shardrpc.DeleteShardArgs); ok {
		args = p
	} else {
		val := req.(shardrpc.DeleteShardArgs)
		args = &val
	}
	err := kv.deleteShard(args.Shard, args.Num)
	return &shardrpc.DeleteShardReply{Err: err}
}

func (kv *ShardMemoryKV) get(key string) (string, rpc.Tversion, rpc.Err) {
	shard := shardcfg.Key2Shard(key)
	if !kv.shardExts[shard] {
		log.Printf("ShardMemoryKV%d-%d.Get()|ErrWrongGroup|key=%s|shard=%d", kv.gid, kv.me, key, shard)
		return "", 0, rpc.ErrWrongGroup
	}
	data := kv.data[shard]

	record, ok := data[key]
	if !ok {
		log.Printf("ShardMemoryKV%d-%d.Get()|ErrNoKey|key=%s|shard=%d", kv.gid, kv.me, key, shard)
		return "", 0, rpc.ErrNoKey
	}

	log.Printf("ShardMemoryKV%d-%d.Get()|OK|key=%s|shard=%d|value=%s|version=%d", kv.gid, kv.me, key, shard, record.Value, record.Version)
	return record.Value, record.Version, rpc.OK
}

func (kv *ShardMemoryKV) put(key string, value string, version rpc.Tversion) rpc.Err {
	shard := shardcfg.Key2Shard(key)
	if !kv.shardExts[shard] {
		log.Printf("ShardMemoryKV%d-%d.Put()|ErrWrongGroup|key=%s|shard=%d|value=%v|version=%d", kv.gid, kv.me, key, shard, value, version)
		return rpc.ErrWrongGroup
	}
	data := kv.data[shard]

	record, ok := data[key]
	if !ok && version != 0 {
		log.Printf("ShardMemoryKV%d-%d.Put()|ErrNoKey|key=%s|shard=%d|value=%v|version=%d", kv.gid, kv.me, key, shard, value, version)
		return rpc.ErrNoKey
	}
	if ok && version != record.Version {
		log.Printf("ShardMemoryKV%d-%d.Put()|ErrVersion|key=%s|shard=%d|value=%v|want_version=%d|given_version=%d", kv.gid, kv.me, key, shard, record.Value, record.Version, version)
		return rpc.ErrVersion
	}

	record.Value = value
	record.Version++
	data[key] = record

	log.Printf("ShardMemoryKV%d-%d.Put()|OK|key=%s|shard=%d|value=%v|version=%d", kv.gid, kv.me, key, shard, value, version)
	return rpc.OK
}

func (kv *ShardMemoryKV) freezeShard(shard shardcfg.Tshid, num shardcfg.Tnum) ([]byte, shardcfg.Tnum, rpc.Err) {
	if num < kv.shardNums[shard] {
		log.Printf("ShardMemoryKV%d-%d.FreezeShard()|ErrWrongGroup|shard=%d|given_num=%d|current_num=%d", kv.gid, kv.me, shard, num, kv.shardNums[shard])
		return nil, kv.shardNums[shard], rpc.ErrWrongGroup
	}
	kv.shardNums[shard] = num

	kv.shardExts[shard] = false
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	e.Encode(kv.data[shard])
	log.Printf("ShardMemoryKV%d-%d.FreezeShard()|OK|shard=%d|num=%d", kv.gid, kv.me, shard, num)
	return b.Bytes(), num, rpc.OK
}

func (kv *ShardMemoryKV) installShard(shard shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	if num < kv.shardNums[shard] {
		log.Printf("ShardMemoryKV%d-%d.InstallShard()|ErrWrongGroup|shard=%d|given_num=%d|current_num=%d", kv.gid, kv.me, shard, num, kv.shardNums[shard])
		return rpc.ErrWrongGroup
	}
	if num == kv.shardNums[shard] {
		log.Printf("ShardMemoryKV%d-%d.InstallShard()|Repeat|shard=%d|num=%d", kv.gid, kv.me, shard, num)
		return rpc.OK
	}
	kv.shardNums[shard] = num

	kv.shardExts[shard] = true
	if len(state) == 0 {
		kv.data[shard] = make(map[string]Record)
	} else {
		var shardData map[string]Record
		b := bytes.NewBuffer(state)
		d := labgob.NewDecoder(b)
		if d.Decode(&shardData) == nil {
			kv.data[shard] = shardData
		}
	}

	log.Printf("ShardMemoryKV%d-%d.InstallShard()|OK|shard=%d|num=%d", kv.gid, kv.me, shard, num)
	return rpc.OK
}

func (kv *ShardMemoryKV) deleteShard(shard shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	if num < kv.shardNums[shard] {
		log.Printf("ShardMemoryKV%d-%d.DeleteShard()|ErrWrongGroup|shard=%d|given_num=%d|current_num=%d", kv.gid, kv.me, shard, num, kv.shardNums[shard])
		return rpc.ErrWrongGroup
	}
	kv.shardNums[shard] = num

	kv.data[shard] = make(map[string]Record)
	log.Printf("ShardMemoryKV%d-%d.DeleteShard()|OK|shard=%d|num=%d", kv.gid, kv.me, shard, num)
	return rpc.OK
}

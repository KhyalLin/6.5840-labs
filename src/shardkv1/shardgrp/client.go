package shardgrp

import (
	"log"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	Interval = time.Millisecond * 100
)

type Clerk struct {
	clnt      *tester.Clnt
	gid       tester.Tgid
	servers   []string
	prvLeader int
}

func MakeClerk(clnt *tester.Clnt, gid tester.Tgid, servers []string) *Clerk {
	ck := &Clerk{
		clnt:      clnt,
		gid:       gid,
		servers:   servers,
		prvLeader: -1,
	}
	return ck
}

func (ck *Clerk) peer() int {
	if ck.prvLeader >= 0 {
		return ck.prvLeader
	}
	return rand.Intn(len(ck.servers))
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	log.Printf("GroupClerk%d.Get()|Start|key=%s|shard=%d", ck.gid, key, shardcfg.Key2Shard(key))

	args := &rpc.GetArgs{Key: key}
	ddl := time.Now().Add(2 * time.Second)

	for time.Now().Before(ddl) {
		reply := &rpc.GetReply{}
		peer := ck.peer()
		ok := ck.clnt.Call(ck.servers[peer], "KVServer.Get", args, reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.prvLeader = -1
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.Get()|End|key=%s|shard=%d|value=%s|version=%d|err=%v", ck.gid, key, shardcfg.Key2Shard(key), reply.Value, reply.Version, reply.Err)
		return reply.Value, reply.Version, reply.Err
	}
	return "", 0, rpc.ErrWrongGroup
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion, retry *Retry) rpc.Err {
	log.Printf("GroupClerk%d.Put()|Start|key=%s|shard=%d|value=%s|version=%d", ck.gid, key, shardcfg.Key2Shard(key), value, version)

	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	ddl := time.Now().Add(4 * time.Second)
	
	for time.Now().Before(ddl) {
		reply := &rpc.PutReply{}
		peer := ck.peer()
		ok := ck.clnt.Call(ck.servers[peer], "KVServer.Put", args, reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.prvLeader = -1
			retry.Set()
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.Put()|End|key=%s|shard=%d|value=%v|version=%d|err=%v", ck.gid, key, shardcfg.Key2Shard(key), value, version, reply.Err)
		return reply.Err
	}
	return rpc.ErrWrongGroup
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, shardcfg.Tnum, rpc.Err) {
	args := &shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	ddl := time.Now().Add(4 * time.Second)

	log.Printf("GroupClerk%d.FreezeShard()|Start|shard=%d|num=%d", ck.gid, s, num)
	for time.Now().Before(ddl) {
		reply := &shardrpc.FreezeShardReply{}
		peer := ck.peer()
		ok := ck.clnt.Call(ck.servers[peer], "KVServer.FreezeShard", args, reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.prvLeader = -1
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.FreezeShard()|End|shard=%d|num=%d|err=%v", ck.gid, s, num, reply.Err)
		return reply.State, reply.Num, reply.Err
	}
	return nil, 0, rpc.ErrWrongGroup
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := &shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	ddl := time.Now().Add(4 * time.Second)

	log.Printf("GroupClerk%d.InstallShard()|Start|shard=%d|state=%v|num=%d", ck.gid, s, state, num)
	for time.Now().Before(ddl) {
		reply := &shardrpc.InstallShardReply{}
		peer := ck.peer()
		ok := ck.clnt.Call(ck.servers[peer], "KVServer.InstallShard", args, reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.prvLeader = -1
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.InstallShard()|End|shard=%d|num=%d|err=%v", ck.gid, s, num, reply.Err)
		return reply.Err
	}
	return rpc.ErrWrongGroup
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := &shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	ddl := time.Now().Add(4 * time.Second)

	log.Printf("GroupClerk%d.DeleteShard()|Start|shard=%d|num=%d", ck.gid, s, num)
	for time.Now().Before(ddl) {
		reply := &shardrpc.DeleteShardReply{}
		peer := ck.peer()
		ok := ck.clnt.Call(ck.servers[peer], "KVServer.DeleteShard", args, reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.prvLeader = -1
			time.Sleep(Interval)
			continue
		}
		ck.prvLeader = peer
		log.Printf("GroupClerk%d.DeleteShard()|End|shard=%d|num=%d|err=%v", ck.gid, s, num, reply.Err)
		return reply.Err
	}
	return rpc.ErrWrongGroup
}

type Retry struct {
	r bool
}

func (r *Retry) Set() {
	r.r = true
}

func (r *Retry) Get() bool {
	return r.r
}
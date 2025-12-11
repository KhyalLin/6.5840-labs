package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler

	cfg       *shardcfg.ShardConfig
	grpClerks map[tester.Tgid]*shardgrp.Clerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:      clnt,
		sck:       sck,
		cfg:       nil,
		grpClerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	return ck
}

func (ck *Clerk) grpClerk(key string) *shardgrp.Clerk {
	gid, srvs, _ := ck.cfg.GidServers(shardcfg.Key2Shard(key))
	grpClerk, ok := ck.grpClerks[gid]
	if !ok {
		grpClerk = shardgrp.MakeClerk(ck.clnt, gid, srvs)
		ck.grpClerks[gid] = grpClerk
	}
	return grpClerk
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		if ck.cfg == nil {
			log.Printf("KVClerk.Get()|UpdateConfig|key=%s|shard=%d", key, shardcfg.Key2Shard(key))
			ck.cfg = ck.sck.Query()
			ck.grpClerks = make(map[tester.Tgid]*shardgrp.Clerk)
		}

		value, version, err := ck.grpClerk(key).Get(key)
		if err == rpc.ErrWrongGroup {
			ck.cfg = nil
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	var retry shardgrp.Retry
	for {
		if ck.cfg == nil {
			log.Printf("KVClerk.Put()|UpdateConfig|key=%s|shard=%d|value=%v|version=%d", key, shardcfg.Key2Shard(key), value, version)
			ck.cfg = ck.sck.Query()
			ck.grpClerks = make(map[tester.Tgid]*shardgrp.Clerk)
		}

		err := ck.grpClerk(key).Put(key, value, version, &retry)
		if err == rpc.ErrWrongGroup {
			ck.cfg = nil
			retry.Set()
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if err == rpc.ErrVersion && retry.Get() {
			err = rpc.ErrMaybe
		}
		return err
	}
}

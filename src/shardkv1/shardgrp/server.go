package shardgrp

import (
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	sm rsm.StateMachine
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep := result.(*rpc.GetReply)
	reply.Value = rep.Value
	reply.Version = rep.Version
	reply.Err = rep.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	reply.Err = result.(*rpc.PutReply).Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep := result.(*shardrpc.FreezeShardReply)
	reply.State = rep.State
	reply.Num = rep.Num
	reply.Err = rep.Err
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	reply.Err = result.(*shardrpc.InstallShardReply).Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, result := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	reply.Err = result.(*shardrpc.DeleteShardReply).Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	sm := NewShardMemoryKV(gid, me)
	kv := &KVServer{
		me:  me,
		rsm: rsm.MakeRSM(servers, me, persister, maxraftstate, sm),
		sm:  sm,
	}
	return []tester.IService{kv, kv.rsm.Raft()}
}

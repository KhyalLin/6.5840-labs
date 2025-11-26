package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Req any
	Me  int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	dead     int32
	term     int
	isLeader bool
	notifyCh map[int]chan any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,

		notifyCh: make(map[int]chan any),
	}

	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}

	rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	go rsm.reader()
	go rsm.watcher()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) killed() bool {
	return atomic.LoadInt32(&rsm.dead) != 0
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	op := Op{
		Req: req,
		Me:  rsm.me,
	}
	index, _, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan any, 1)
	rsm.notifyCh[index] = ch
	rsm.mu.Unlock()

	reply := <-ch
	if reply == nil {
		return rpc.ErrWrongLeader, nil
	} else {
		return rpc.OK, reply
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			rsm.handleCommand(msg.Command.(Op), msg.CommandIndex)
		}
		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
		}

		if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
			snapshot := rsm.sm.Snapshot()
			rsm.rf.Snapshot(msg.CommandIndex, snapshot)
		}
	}

	rsm.mu.Lock()
	atomic.StoreInt32(&rsm.dead, 1)
	for index, ch := range rsm.notifyCh {
		ch <- nil
		delete(rsm.notifyCh, index)
	}
	rsm.mu.Unlock()
}

func (rsm *RSM) handleCommand(op Op, index int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	resp := rsm.sm.DoOp(op.Req)
	ch, ok := rsm.notifyCh[index]
	if !ok {
		return
	}

	if op.Me != rsm.me {
		ch <- nil
	} else {
		ch <- resp
	}
	delete(rsm.notifyCh, index)
}

func (rsm *RSM) watcher() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if rsm.killed() {
			return
		}

		term, isLeader := rsm.rf.GetState()
		rsm.mu.Lock()
		if term != rsm.term || isLeader != rsm.isLeader {
			for _, ch := range rsm.notifyCh {
				ch <- nil
			}
			rsm.notifyCh = make(map[int]chan any)
		}
		rsm.term, rsm.isLeader = term, isLeader
		rsm.mu.Unlock()
	}
}

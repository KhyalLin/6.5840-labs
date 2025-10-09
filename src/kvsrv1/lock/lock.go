package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	ck kvtest.IKVClerk
	id string
	name string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		id: kvtest.RandValue(8),
		name: l,
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.name)
		if err == rpc.OK && value != "" {
			time.Sleep(time.Second * 3)
			continue
		}
		err = lk.ck.Put(lk.name, lk.id, version)
		if err == rpc.ErrVersion {
			time.Sleep(time.Millisecond * 3)
			continue
		}
		if err == rpc.OK {
			return
		}
	}
}

func (lk *Lock) Release() {
	for {
		_, version, _ := lk.ck.Get(lk.name)
		err := lk.ck.Put(lk.name, "", version)
		if err == rpc.OK {
			return
		}
	}
}

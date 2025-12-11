package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	grpClerks map[tester.Tgid]*shardgrp.Clerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{
		clnt:      clnt,
		grpClerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	log.Printf("ShardCtrler.InitController()|Start")

	new, newVersion := sck.getNextConfig()
	if new == nil {
		return
	}
	old, oldVersion := sck.getCurConfig()

	ok := sck.moveShards(old, new)
	if !ok {
		log.Printf("ShardCtrler.InitController()|Fail|To=%s", new.String())
		return
	}

	err := sck.putCurConfig(new, oldVersion)
	if err == rpc.ErrVersion {
		log.Printf("ShardCtrler.InitController()|InProgress|To=%s", new.String())
		return
	}

	sck.deleteNextConfig(newVersion)
	log.Printf("ShardCtrler.InitController()|End|To=%s", new.String())
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	for {
		err := sck.Put("config", cfg.String(), 0)
		if err == rpc.ErrMaybe {
			value, _, err := sck.Get("config")
			if err == rpc.OK && value == cfg.String() {
				break
			}
			continue
		}
		if err == rpc.OK {
			break
		}
	}
	log.Printf("ShardCtrler.InitConfig()|OK|Config=%s", cfg.String())
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	old, oldVersion := sck.getCurConfig()
	log.Printf("ShardCtrler.ChangeConfigTo()|Start|From=%s|To=%s", old.String(), new.String())

	next, newVersion := sck.getNextConfig()
	if next != nil {
		log.Printf("ShardCtrler.ChangeConfigTo()|InProgress|To=%s", new.String())
		return
	}
	err := sck.putNextConfig(new, newVersion)
	if err == rpc.ErrVersion {
		log.Printf("ShardCtrler.ChangeConfigTo()|InProgress|To=%s", new.String())
		return
	}
	newVersion++

	ok := sck.moveShards(old, new)
	if !ok {
		log.Printf("ShardCtrler.ChangeConfigTo()|Fail|To=%s", new.String())
		return
	}

	err = sck.putCurConfig(new, oldVersion)
	if err == rpc.ErrVersion {
		log.Printf("ShardCtrler.ChangeConfigTo()|InProgress|To=%s", new.String())
		return
	}
	
	sck.deleteNextConfig(newVersion)
	log.Printf("ShardCtrler.ChangeConfigTo()|End|To=%s", new.String())
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	cfg, _ := sck.getCurConfig()
	return cfg
}

func (sck *ShardCtrler) moveShards(old, new *shardcfg.ShardConfig) bool {
	log.Printf("ShardCtrler.moveShards()|From=%s|To=%s", old.String(), new.String())
	for i := 0; i < shardcfg.NShards; i++ {
		shard := shardcfg.Tshid(i)
		oldGid, oldsrv, _ := old.GidServers(shard)
		newGid, newsrv, _ := new.GidServers(shard)
		if oldGid == newGid {
			continue
		}

		oldClerk, ok := sck.grpClerks[oldGid]
		if !ok {
			oldClerk = shardgrp.MakeClerk(sck.clnt, oldGid, oldsrv)
			sck.grpClerks[oldGid] = oldClerk
		}
		newClerk, ok := sck.grpClerks[newGid]
		if !ok {
			newClerk = shardgrp.MakeClerk(sck.clnt, newGid, newsrv)
			sck.grpClerks[newGid] = newClerk
		}

		state, _, err := oldClerk.FreezeShard(shard, new.Num)
		if err != rpc.OK {
			log.Printf("ShardCtrler.moveShards()|FreezeShard|Fail|shard=%d|err=%v", shard, err)
			return false
		}
		err = newClerk.InstallShard(shard, state, new.Num)
		if err != rpc.OK {
			log.Printf("ShardCtrler.moveShards()|InstallShard|Fail|shard=%d|err=%v", shard, err)
			return false
		}
		err = oldClerk.DeleteShard(shard, new.Num)
		if err != rpc.OK {
			log.Printf("ShardCtrler.moveShards()|DeleteShard|Fail|shard=%d|err=%v", shard, err)
			return false
		}
	}
	return true
}

func (sck *ShardCtrler) getCurConfig() (*shardcfg.ShardConfig, rpc.Tversion) {
	value, version, err := sck.Get("config")
	if err != rpc.OK {
		log.Fatalf("ShardCtrler.getCurConfig()|Get|Fail|err=%v", err)
	}
	cfg := shardcfg.FromString(value)
	log.Printf("ShardCtrler.getCurConfig()|OK|Config=%s", cfg.String())
	return cfg, version
}

func (sck *ShardCtrler) getNextConfig() (*shardcfg.ShardConfig, rpc.Tversion) {
	value, version, err := sck.Get("next")
	if err == rpc.ErrNoKey || value == "" {
		return nil, version
	}
	if err != rpc.OK {
		log.Fatalf("ShardCtrler.getNextConfig()|Fail|err=%v", err)
	}
	cfg := shardcfg.FromString(value)
	log.Printf("ShardCtrler.getNextConfig()|OK|Config=%s", cfg.String())
	return cfg, version
}

func (sck *ShardCtrler) putCurConfig(cfg *shardcfg.ShardConfig, version rpc.Tversion) rpc.Err {
	log.Printf("ShardCtrler.putCurConfig()|Start|cfg=%s|version=%d", cfg.String(), version)
	value := cfg.String()
	err := sck.Put("config", value, version)
	if err == rpc.ErrMaybe {
		val, _, err := sck.Get("config")
		if err != rpc.OK {
			log.Fatalf("ShardCtrler.putCurConfig()|Fail")
		}
		if value == val {
			err = rpc.OK
		} else {
			err = rpc.ErrVersion
		}
	}
	log.Printf("ShardCtrler.putCurConfig()|End|cfg=%s|version=%d|err=%v", cfg.String(), version, err)
	return err
}

func (sck *ShardCtrler) putNextConfig(cfg *shardcfg.ShardConfig, version rpc.Tversion) rpc.Err {
	log.Printf("ShardCtrler.putNextConfig()|Start|cfg=%s|version=%d", cfg.String(), version)
	value := cfg.String()
	err := sck.Put("next", value, version)
	if err == rpc.ErrMaybe {
		val, _, err := sck.Get("next")
		if err != rpc.OK {
			log.Fatalf("")
		}
		if value == val {
			err = rpc.OK
		} else {
			err = rpc.ErrVersion
		}
	}
	log.Printf("ShardCtrler.putNextConfig()|End|cfg=%s|version=%d|err=%v", cfg.String(), version, err)
	return err
}

func (sck *ShardCtrler) deleteNextConfig(version rpc.Tversion) rpc.Err {
	log.Printf("ShardCtrler.deleteNextConfig()|Start|version=%d", version)
	err := sck.Put("next", "", version)
	if err == rpc.ErrMaybe {
		value, _, err := sck.Get("next")
		if err != rpc.OK {
			log.Fatalf("")
		}
		if value == "" {
			err = rpc.OK
		} else {
			err = rpc.ErrVersion
		}
	}
	log.Printf("ShardCtrler.deleteNextConfig()|End|err=%v", err)
	return err
}
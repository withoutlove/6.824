package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]*ValueEntry
}

type ValueEntry struct {
	value   string
	version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.data = make(map[string]*ValueEntry)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, exists := kv.data[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = entry.value
	reply.Version = entry.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, exists := kv.data[args.Key]
	if !exists {
		// Key doesn't exist
		if args.Version == 0 {
			// Install new key-value pair with version 1
			kv.data[args.Key] = &ValueEntry{
				value:   args.Value,
				version: 1,
			}
			reply.Err = rpc.OK
		} else {
			// Version != 0 but key doesn't exist
			reply.Err = rpc.ErrNoKey
		}
		return
	}

	// Key exists, check version
	if entry.version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	// Version matches, update value and increment version
	entry.value = args.Value
	entry.version++
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}

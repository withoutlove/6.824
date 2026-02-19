package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	var reply rpc.GetReply

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", args, &reply)
		if !ok {
			// RPC失败，继续重试
			continue
		}

		// 检查错误类型
		if reply.Err == rpc.ErrNoKey {
			// 键不存在，直接返回错误
			return "", 0, rpc.ErrNoKey
		} else if reply.Err == rpc.OK {
			// 成功，返回值和版本
			return reply.Value, reply.Version, rpc.OK
		}
		// 其他错误（理论上不应该有其他错误），继续重试
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	var reply rpc.PutReply

	firstTry := true

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", args, &reply)
		if !ok {
			// RPC失败，继续重试
			firstTry = false
			continue
		}

		// 检查服务器返回的错误
		switch reply.Err {
		case rpc.OK:
			// 成功
			return rpc.OK
		case rpc.ErrNoKey:
			// 键不存在（版本不为0但键不存在）
			return rpc.ErrNoKey
		case rpc.ErrVersion:
			// 版本不匹配
			if firstTry {
				// 第一次尝试就收到ErrVersion，确定失败
				return rpc.ErrVersion
			} else {
				// 重试时收到ErrVersion，可能已成功
				return rpc.ErrMaybe
			}
		default:
			// 其他错误，继续重试
			firstTry = false
		}
	}
}

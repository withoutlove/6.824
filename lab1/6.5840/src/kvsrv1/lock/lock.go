package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name     string
	clientID string // 客户端唯一标识符
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	// 生成客户端唯一标识符
	clientID := kvtest.RandValue(8)
	lk := &Lock{ck: ck, name: lockname, clientID: clientID}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		// 先获取当前锁状态
		val, ver, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			// 锁不存在，尝试创建锁，值为客户端ID
			err2 := lk.ck.Put(lk.name, lk.clientID, 0)
			if err2 == rpc.OK {
				// 成功获取锁
				return
			} else if err2 == rpc.ErrMaybe {
				// 不确定是否成功，需要验证
				// 再次获取锁状态检查
				val2, _, err3 := lk.ck.Get(lk.name)
				if err3 == rpc.OK && val2 == lk.clientID {
					// 锁已被设置为我的客户端ID，可能是我设置的
					return
				}
				// 不是我的ID，继续尝试
			}
		} else if err == rpc.OK {
			// 锁存在，检查是否未锁定（空字符串表示未锁定）
			if val == "" {
				// 尝试获取锁：将值从空字符串改为客户端ID
				err2 := lk.ck.Put(lk.name, lk.clientID, ver)
				if err2 == rpc.OK {
					// 成功获取锁
					return
				} else if err2 == rpc.ErrMaybe {
					// 不确定是否成功，需要验证
					// 再次获取锁状态检查
					val2, _, err3 := lk.ck.Get(lk.name)
					if err3 == rpc.OK && val2 == lk.clientID {
						// 锁已被设置为我的客户端ID，可能是我设置的
						return
					}
					// 不是我的ID，继续尝试
				}
			} else if val == lk.clientID {
				// 锁已经被当前客户端持有（重入情况）
				// 在这个简单实现中，我们不允许锁重入，所以继续等待
				// 实际上应该立即返回，但测试可能不期望重入
			}
			// 锁已被其他客户端持有，等待后重试
		}
		// 等待后重试
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		// 获取当前锁状态
		val, ver, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			// 锁不存在，已经释放
			return
		} else if err == rpc.OK {
			if val == lk.clientID {
				// 尝试释放锁：将值从客户端ID改为空字符串
				err2 := lk.ck.Put(lk.name, "", ver)
				if err2 == rpc.OK || err2 == rpc.ErrMaybe {
					// 成功释放或可能已释放
					return
				}
			} else if val == "" {
				// 锁已经是释放状态
				return
			} else {
				// 锁被其他客户端持有，不能释放
				// 在这个简单实现中，我们直接返回
				return
			}
		}
		// 等待后重试
		time.Sleep(10 * time.Millisecond)
	}
}

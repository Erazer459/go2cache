package go2cache

import (
	"log"
	"sync"
	"time"
)

type CacheTable struct {
	sync.RWMutex

	name string

	cleanupTimer    *time.Timer
	cleanupInterval time.Duration

	logger *log.Logger
	//没有hit时的回调函数(可用作从数据库中引入数据)
	loadData          func(key interface{}, args ...interface{}) *CacheItem
	addedItem         []func(item *CacheItem)
	aboutToDeleteItem []func(item *CacheItem)
}

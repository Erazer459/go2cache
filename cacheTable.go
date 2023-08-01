package go2cache

import (
	"log"
	"sort"
	"sync"
	"time"
)

type CacheTable struct {
	sync.RWMutex

	name            string
	items           map[interface{}]*CacheItem
	cleanupTimer    *time.Timer
	cleanupInterval time.Duration

	logger *log.Logger
	//没有hit时的回调函数(可用作从数据库中引入数据)
	loadData          func(key interface{}, args ...interface{}) *CacheItem
	addedItem         []func(item *CacheItem)
	aboutToDeleteItem []func(item *CacheItem)
}

func (table *CacheTable) Count() int {
	table.RLock()
	defer table.Unlock()
	return len(table.items)
}
func (table *CacheTable) Foreach(trans func(key interface{}, item *CacheItem)) {
	table.RLock()
	defer table.RUnlock()
	for k, v := range table.items {
		trans(k, v)
	}
}
func (table *CacheTable) SetDataLoader(f func(interface{}, ...interface{}) *CacheItem) {
	table.Lock()
	defer table.Unlock()
	table.loadData = f
}
func (table *CacheTable) SetAddedItemCallback(f func(*CacheItem)) { //覆盖add回调函数
	if len(table.addedItem) > 0 {
		table.RemoveAddedItemCallbacks()
	}
	table.Lock()
	defer table.Unlock()
	table.addedItem = append(table.addedItem, f)
}

func (table *CacheTable) RemoveAddedItemCallbacks() { //移除add回调函数
	table.Lock()
	defer table.Unlock()
	table.addedItem = nil
}

func (table *CacheTable) AddAddedItemCallback(f func(*CacheItem)) { //添加add回调函数
	table.Lock()
	defer table.Unlock()
	table.addedItem = append(table.addedItem, f)
}
func (table *CacheTable) SetAboutToDeleteItemCallback(f func(*CacheItem)) {
	if len(table.aboutToDeleteItem) > 0 {
		table.RemoveAboutToDeleteItemCallback()
	}
	table.Lock()
	defer table.Unlock()
	table.aboutToDeleteItem = append(table.aboutToDeleteItem, f)
}

func (table *CacheTable) RemoveAboutToDeleteItemCallback() {
	table.Lock()
	defer table.Unlock()
	table.aboutToDeleteItem = nil
}
func (table *CacheTable) AddAboutToDeleteItemCallback(f func(item *CacheItem)) {
	table.Lock()
	defer table.Unlock()
	table.aboutToDeleteItem = append(table.aboutToDeleteItem, f)
}
func (table *CacheTable) SetLogger(logger *log.Logger) {
	table.Lock()
	defer table.Unlock()
	table.logger = logger
}
func (table *CacheTable) expirationCheck() {
	table.Lock()
	if table.cleanupTimer != nil {
		table.cleanupTimer.Stop()
	}
	if table.cleanupInterval > 0 {
		table.log("key过期检查after", table.cleanupInterval, "table:", table.name)
	} else {
		table.log("key过期检查装载,table:", table.name)
	}
	now := time.Now()
	smallestDuration := 0 * time.Second
	for key, item := range table.items {
		item.RLock()
		lifeSpan := item.lifeSpan
		accessedOn := item.accessedOn
		item.RUnlock()
		if lifeSpan == 0 {
			continue
		}
		if now.Sub(accessedOn) >= lifeSpan {
			table.deleteInternal(key)
		} else {
			if smallestDuration == 0 || lifeSpan-now.Sub(accessedOn) < smallestDuration {
				smallestDuration = lifeSpan - now.Sub(accessedOn)
			}
		}
	}
	table.cleanupInterval = smallestDuration
	if smallestDuration > 0 {
		table.cleanupTimer = time.AfterFunc(smallestDuration, func() {
			go table.expirationCheck()
		})
	}
	table.Unlock()
}

func (table *CacheTable) log(v ...interface{}) {
	if table.logger == nil {
		return
	}
	table.logger.Println(v...)
}

func (table *CacheTable) deleteInternal(key interface{}) (*CacheItem, error) {
	r, ok := table.items[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	callbacks := table.aboutToDeleteItem
	table.Unlock()
	if callbacks != nil {
		for _, callback := range callbacks {
			callback(r)
		}
	}
	r.RLock()
	defer r.RUnlock()
	if r.aboutToExpire != nil {
		for _, callback := range r.aboutToExpire {
			callback(key)
		}
	}
	table.Lock()
	table.log("删除key:", key, "创建于:", r.createdOn, "命中次数:", r.accessCount, "所在表:", table.name)
	delete(table.items, key)
	return r, nil
}
func (table *CacheTable) addInternal(item *CacheItem) { //除非table锁住否则使用Add
	table.log("添加缓存，key:", item.key, "存活时间:", item.lifeSpan)
	table.items[item.key] = item
	expDur := table.cleanupInterval
	addedItem := table.addedItem
	table.Unlock()
	if addedItem != nil {
		for _, callback := range addedItem {
			callback(item)
		}
	}
	if item.lifeSpan > 0 && (expDur == 0 || item.lifeSpan < expDur) {
		table.expirationCheck()
	}
}
func (table *CacheTable) Add(key interface{}, lifeSpan time.Duration, data interface{}) *CacheItem {
	item := NewCacheItem(key, lifeSpan, data)
	table.Lock()
	table.addInternal(item)
	return item
}
func (table *CacheTable) Delete(key interface{}) (*CacheItem, error) {
	table.Lock()
	defer table.Unlock()
	return table.deleteInternal(key)
}
func (table *CacheTable) Exists(key interface{}) bool {
	table.RLock()
	defer table.RUnlock()
	_, ok := table.items[key]
	return ok
}
func (table *CacheTable) NotFoundAdd(key interface{}, lifeSpan time.Duration, data interface{}) bool {
	table.Lock()
	if _, ok := table.items[key]; ok {
		table.Unlock()
		return false
	}
	item := NewCacheItem(key, lifeSpan, data)
	table.addInternal(item)
	return true
}
func (table *CacheTable) Value(key interface{}, args ...interface{}) (*CacheItem, error) {
	table.RLock()
	r, ok := table.items[key]
	loadData := table.loadData
	table.RUnlock()
	if ok {
		r.KeepAlive()
		return r, nil
	}
	if loadData != nil {
		item := loadData(key, args)
		if item != nil {
			table.Add(key, item.lifeSpan, item.data)
			return item, nil
		}
		return nil, ErrKeyNotFoundOrLoadable
	}
	return nil, ErrKeyNotFound
}
func (table *CacheTable) Flush() { //清空表中所有的缓存项
	table.Lock()
	defer table.Unlock()
	table.log("清空表:", table.name)
	table.items = make(map[interface{}]*CacheItem)
	table.cleanupInterval = 0
	if table.cleanupTimer != nil {
		table.cleanupTimer.Stop()
	}

}

type CacheItemPair struct {
	Key         interface{}
	AccessCount int64
}
type CacheItemPairList []CacheItemPair

func (p CacheItemPairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] } //继承sort类内部方法
func (p CacheItemPairList) Len() int           { return len(p) }
func (p CacheItemPairList) Less(i, j int) bool { return p[i].AccessCount > p[j].AccessCount }
func (table *CacheTable) MostAccessed(count int64) []*CacheItem { //寻找hit数大于count的key
	table.RLock()
	defer table.RUnlock()

	p := make(CacheItemPairList, len(table.items))
	i := 0
	for k, v := range table.items {
		p[i] = CacheItemPair{k, v.accessCount}
		i++
	}
	sort.Sort(p)

	var r []*CacheItem
	c := int64(0)
	for _, v := range p {
		if c >= count {
			break
		}

		item, ok := table.items[v.Key]
		if ok {
			r = append(r, item)
		}
		c++
	}

	return r
}

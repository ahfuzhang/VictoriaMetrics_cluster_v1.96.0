package logstorage

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
)

type cache struct {
	curr atomic.Pointer[sync.Map]
	prev atomic.Pointer[sync.Map]

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newCache() *cache {
	var c cache
	c.curr.Store(&sync.Map{}) // 为了方便索引切换，所以做成这样
	c.prev.Store(&sync.Map{})

	c.stopCh = make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runCleaner() // 在协程内定期清理
	}()
	return &c
}

func (c *cache) MustStop() {
	close(c.stopCh)
	c.wg.Wait()
}

func (c *cache) runCleaner() { // 在协程内定期清理
	// 猜测，抖动是为了避免所有节点同时一瞬间都过期了
	d := timeutil.AddJitterToDuration(3 * time.Minute) // 产生一个 3 分钟左右的随机间隔
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.clean() // 三分钟清理一次.
		case <-c.stopCh:
			return
		}
	}
}

func (c *cache) clean() { // 3 分钟清理一次
	curr := c.curr.Load()
	c.prev.Store(curr)
	c.curr.Store(&sync.Map{}) // 为什么 3 分钟就要丢弃一次索引?  如果缓存太长，就会导致新出现的 stream id 无法被搜索到
}

func (c *cache) Get(k []byte) (any, bool) {
	kStr := bytesutil.ToUnsafeString(k)

	curr := c.curr.Load()
	v, ok := curr.Load(kStr) // 先在当前索引找
	if ok {
		return v, true
	}

	prev := c.prev.Load()
	v, ok = prev.Load(kStr) // 找不到就去前一个索引找  // 所以索引有效时间为 6 分钟
	if ok {
		kStr = strings.Clone(kStr)
		curr.Store(kStr, v)
		return v, true
	}
	return nil, false
}

func (c *cache) Set(k []byte, v any) {
	kStr := string(k)
	curr := c.curr.Load()
	curr.Store(kStr, v)
}

package rwcounter

// v0.0.1
import (
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct {
	pool          []map[string]*CounterItem
	mutex         []sync.RWMutex
	windowSize    int
	currentWindow int
}

type CounterItem struct {
	Count         *int64
	InternalCount *int64
	Last          time.Time
	windows       map[int]*int64
}

type CounterCfg struct {
	GCInterval time.Duration
	TTL        time.Duration
	PoolSize   int
	Windows    int
}

func NewCounter(cfg CounterCfg) *Counter {
	c := &Counter{
		pool:          make([]map[string]*CounterItem, cfg.PoolSize),
		mutex:         make([]sync.RWMutex, cfg.PoolSize),
		windowSize:    cfg.Windows,
		currentWindow: time.Now().Second() % cfg.Windows,
	}

	for i := 0; i < cfg.PoolSize; i++ {
		c.pool[i] = make(map[string]*CounterItem)
	}

	go func() {
		t := time.NewTicker(time.Second * cfg.GCInterval)
		for {
			<-t.C
			for i, m := range c.pool {
				c.mutex[i].Lock()
				for k, v := range m {
					if time.Since(v.Last) > cfg.TTL {
						delete(m, k)
					}
				}
				c.mutex[i].Unlock()
			}
		}
	}()

	go func() {
		t := time.NewTicker(time.Second)
		for {
			<-t.C
			// currentWindow :=
			c.currentWindow = time.Now().Second() % cfg.Windows
			nextWindow := c.currentWindow + 1
			if nextWindow >= cfg.Windows {
				nextWindow = 0
			}

			for i, m := range c.pool {
				c.mutex[i].RLock()
				for k, v := range m {
					atomic.StoreInt64(v.Count, atomic.LoadInt64(v.InternalCount))
					windowCount := atomic.LoadInt64(v.windows[nextWindow])
					c.decr(k, windowCount)
					atomic.StoreInt64(v.windows[nextWindow], 0)
				}
				c.mutex[i].RUnlock()
			}
		}
	}()
	return c
}

func (c *Counter) hash(key string) int {
	var sum int64
	for _, c := range key {
		sum += int64(c)
	}
	return int(sum) % len(c.pool)
}

func (c *Counter) Incr(key string, step int64) {
	idx := c.hash(key)
	c.mutex[idx].RLock()
	var counter *CounterItem
	var wCounter *int64
	var ok bool
	counter, ok = c.pool[idx][key]
	if !ok {
		c.mutex[idx].RUnlock()
		c.mutex[idx].Lock()
		counter = &CounterItem{
			Count:         new(int64),
			InternalCount: new(int64),
			Last:          time.Now(),
			windows:       make(map[int]*int64),
		}
		c.pool[idx][key] = counter
		for i := 0; i < c.windowSize; i++ {
			c.pool[idx][key].windows[i] = new(int64)
		}
		wCounter = counter.windows[c.currentWindow]
		c.mutex[idx].Unlock()
	} else {
		wCounter = counter.windows[c.currentWindow]
		c.mutex[idx].RUnlock()
	}

	atomic.AddInt64(counter.InternalCount, step)
	atomic.AddInt64(wCounter, step)
	counter.Last = time.Now()
}

func (c *Counter) Get(key string) int64 {
	idx := c.hash(key)
	c.mutex[idx].RLock()
	defer c.mutex[idx].RUnlock()
	ptr, ok := c.pool[idx][key]
	if !ok {
		return 0
	}
	return atomic.LoadInt64(ptr.Count)
}

func (c *Counter) Keys() []string {
	var keys []string
	for i, m := range c.pool {
		c.mutex[i].RLock()
		for k := range m {
			keys = append(keys, k)
		}
		c.mutex[i].RUnlock()
	}
	return keys
}

func (c *Counter) decr(key string, step int64) {
	idx := c.hash(key)
	c.mutex[idx].RLock()
	defer c.mutex[idx].RUnlock()
	if _, ok := c.pool[idx][key]; ok {
		atomic.AddInt64(c.pool[idx][key].InternalCount, -step)
	}
}

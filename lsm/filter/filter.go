package filter

import (
	"bytes"
	"encoding/gob"
	bits_blooms "github.com/bits-and-blooms/bloom/v3"
	cuckoo "github.com/seiflotfy/cuckoofilter"
	boom "github.com/tylertreat/BoomFilters"
	"sync"
)

// Filter will let you Insert into the data structure and Check membership.
// It will have some false positives. But it must never have false negatives.
//
// All implementations of this interface are expected to be thread safe.
type Filter interface {
	Lookup(inp []byte) bool
	Insert(inp []byte) bool
	Encode() ([]byte, error)
	Init(n uint, fpRate float64) Filter
	Decode([]byte) (Filter, error)
}

type Freezable interface {
	Freeze()
}

type BoomBloomFilter struct {
	// boom is not thread safe. So all operations need to be behind a lock
	bf   *boom.BloomFilter
	lock sync.Mutex
}

func (b *BoomBloomFilter) Init(n uint, fpRate float64) Filter {
	b.bf = boom.NewBloomFilter(n, fpRate)
	return b
}

func (b *BoomBloomFilter) Decode(inp []byte) (Filter, error) {
	b.bf = &boom.BloomFilter{}
	err := b.bf.GobDecode(inp)
	if err != nil {
		return nil, err
	} else {
		return b, nil
	}
}

func (b *BoomBloomFilter) Lookup(data []byte) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.bf.Test(data)
}

func (b *BoomBloomFilter) Insert(data []byte) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.bf.Add(data)
	return true
}

func (b *BoomBloomFilter) Encode() ([]byte, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.bf.GobEncode()
}

type BoomCuckooFilter struct {
	cf   *boom.ScalableBloomFilter
	lock sync.Mutex
}

func (c *BoomCuckooFilter) Init(n uint, fpRate float64) Filter {
	c.cf = boom.NewScalableBloomFilter(n, fpRate, 0.8)
	return c
}

func (c *BoomCuckooFilter) Decode(inp []byte) (Filter, error) {
	dec := gob.NewDecoder(bytes.NewReader(inp))
	if err := dec.Decode(&c.cf); err != nil {
		return nil, err
	} else {
		return c, nil
	}
}

func (c *BoomCuckooFilter) Lookup(inp []byte) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.cf.Test(inp)
}

func (c *BoomCuckooFilter) Insert(inp []byte) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if err := c.cf.Add(inp); err != nil {
		return false
	} else {
		return true
	}
}

func (c *BoomCuckooFilter) Encode() ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(c.cf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type CuckooFilter struct {
	cf     *cuckoo.Filter
	lock   sync.RWMutex
	roMode bool
}

func (c *CuckooFilter) Init(n uint, fpRate float64) Filter {
	c.cf = cuckoo.NewFilter(n)
	return c
}

func (c *CuckooFilter) Freeze() {
	c.roMode = true
}

func (c *CuckooFilter) Decode(inp []byte) (Filter, error) {
	var err error
	c.cf, err = cuckoo.Decode(inp)
	return c, err
}

func (c *CuckooFilter) Lookup(inp []byte) bool {
	if !c.roMode {
		c.lock.RLock()
		defer c.lock.RUnlock()
	}
	return c.cf.Lookup(inp)
}

func (c *CuckooFilter) Insert(inp []byte) bool {
	if c.roMode {
		return false
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.cf.Insert(inp)
}

func (c *CuckooFilter) Encode() ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.cf.Encode(), nil
}

type ScalableCuckooFilter struct {
	cf     *cuckoo.ScalableCuckooFilter
	lock   sync.RWMutex
	roMode bool
}

func (c *ScalableCuckooFilter) Init(uint, float64) Filter {
	c.cf = cuckoo.NewScalableCuckooFilter()
	return c
}

func (c *ScalableCuckooFilter) Decode(inp []byte) (Filter, error) {
	var err error
	c.cf, err = cuckoo.DecodeScalableFilter(inp)
	return c, err
}

func (b *ScalableCuckooFilter) Freeze() {
	b.roMode = true
}

func (c *ScalableCuckooFilter) Lookup(inp []byte) bool {
	if !c.roMode {
		c.lock.RLock()
		defer c.lock.RUnlock()
	}
	return c.cf.Lookup(inp)
}

func (c *ScalableCuckooFilter) Insert(inp []byte) bool {
	if c.roMode {
		return false
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.cf.Insert(inp)
}

func (c *ScalableCuckooFilter) Encode() ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.cf.Encode(), nil
}

type BitsAndBloomsFilter struct {
	bf *bits_blooms.BloomFilter
	// When in roMode, lock will not be checked.
	lock   sync.RWMutex
	roMode bool
}

func (b *BitsAndBloomsFilter) Init(n uint, fpRate float64) Filter {
	b.bf = bits_blooms.NewWithEstimates(n, fpRate)
	return b
}

func (c *BitsAndBloomsFilter) Decode(inp []byte) (Filter, error) {
	c.bf = &bits_blooms.BloomFilter{}
	err := c.bf.GobDecode(inp)
	return c, err
}

func (b *BitsAndBloomsFilter) Lookup(inp []byte) bool {
	if !b.roMode {
		b.lock.RLock()
		defer b.lock.RUnlock()
	}
	return b.bf.Test(inp)
}

func (b *BitsAndBloomsFilter) Insert(inp []byte) bool {
	if b.roMode {
		return false
	}
	b.lock.RLock()
	defer b.lock.RUnlock()
	b.bf.Add(inp)
	return true
}

func (b *BitsAndBloomsFilter) Encode() ([]byte, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.bf.GobEncode()
}

func (b *BitsAndBloomsFilter) Freeze() {
	b.roMode = true
}

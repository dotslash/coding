package tree

import (
	"bytes"
	"github.com/google/btree"
	"sync"
)

const DefaultMaxMemTableSize = int64(32 * 1024 * 1024)
const DefaultSparseIndexMaxGap = int64(16 * 1024)

var defaultMemTableConfig = memTableConfig{
	MaxMemTableSize:      DefaultMaxMemTableSize,
	MaxSparseIndexMapGap: DefaultSparseIndexMaxGap,
}

const (
	memTableErrNotExists                = "Not exists"
	memTableErrNotExistsMarkedAsDeleted = "Marked as deleted"
	memTableErrFull                     = "Mem table full"
)

// === memTableItem begin

type memTableItem struct {
	key       []byte
	value     []byte
	isDeleted bool
}

func (item *memTableItem) size() int {
	return len(item.key) + len(item.value) + 1
}

func memTableItemLess(i1, i2 memTableItem) bool {
	return bytes.Compare(i1.key, i2.key) < 0
}

// === memTableItem end

type memTableConfig struct {
	MaxMemTableSize      int64
	MaxSparseIndexMapGap int64
}

// memTable will be used for reads and writes
// memTable size is tracked via memTableSize and if it goes beyond config.MaxMemTableSize then
type memTable struct {
	// TODO: read write locks

	// ModeInMem related data structures
	data     *btree.BTreeG[memTableItem]
	dataSize int
	// General items
	config     memTableConfig
	tableMutex sync.RWMutex
}

func NewMemTable(conf memTableConfig) *memTable {
	return &memTable{
		data:     btree.NewG(2, memTableItemLess),
		dataSize: 0,
		config:   conf,
	}
}

func (table *memTable) Mode() KVStoreMode {
	return ModeInMem
}
func (table *memTable) isFull() bool {
	return int64(table.dataSize) > table.config.MaxMemTableSize
}

func (table *memTable) putInternal(item memTableItem) DbError {
	table.tableMutex.Lock()
	defer table.tableMutex.Unlock()

	if table.isFull() {
		return NewError(memTableErrFull, InternalError)
	}

	old, oldExists := table.data.ReplaceOrInsert(item)
	if !oldExists {
		table.dataSize += item.size()
	} else {
		table.dataSize += item.size() - old.size()
	}
	return NoError
}

func (table *memTable) Put(key, value string) DbError {
	return table.putInternal(memTableItem{key: []byte(key), value: []byte(value)})
}

func (table *memTable) Delete(key string) DbError {
	return table.putInternal(memTableItem{key: []byte(key), isDeleted: true})
}

func (table *memTable) Get(key string) (value string, dberr DbError) {
	table.tableMutex.RLock()
	defer table.tableMutex.RUnlock()
	existing, ok := table.data.Get(memTableItem{key: []byte(key)})
	if !ok {
		return "", NewError(memTableErrNotExists, KeyNotExists)
	} else if existing.isDeleted {
		return "", NewError(memTableErrNotExistsMarkedAsDeleted, KeyMarkedAsDeleted)
	} else {
		return string(existing.value), NoError
	}
}

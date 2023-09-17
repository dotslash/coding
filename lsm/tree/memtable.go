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

// memTable supports and reads and writes. It is made threadsafe by acquiring mutexes appropriately. If the table size
// goes beyond whats configured in config, it will throw an error. Size is tracked via dataSize.
type memTable struct {
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

func (table *memTable) Put(key, value []byte) DbError {
	return table.putInternal(memTableItem{key: key, value: value})
}

func (table *memTable) Delete(key []byte) DbError {
	return table.putInternal(memTableItem{key: key, isDeleted: true})
}

func (table *memTable) Get(key []byte) (value []byte, dberr DbError) {
	table.tableMutex.RLock()
	defer table.tableMutex.RUnlock()
	existing, ok := table.data.Get(memTableItem{key: key})
	if !ok {
		return nil, NewError(memTableErrNotExists, KeyNotExists)
	} else if existing.isDeleted {
		return nil, NewError(memTableErrNotExistsMarkedAsDeleted, KeyMarkedAsDeleted)
	} else {
		return existing.value, NoError
	}
}

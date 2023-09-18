package tree

import (
	"fmt"
	"os"
	"sync"
	"time"
)

func Empty(dataLocation string) *LSMTree {
	return &LSMTree{
		memTable:     nil,
		ssTables:     make([]*SSTable, 0),
		dataLocation: dataLocation,
	}
}

func LoadLsmTree(dataLocation string) (*LSMTree, DbError) {
	var err error
	entries, err := os.ReadDir(dataLocation)
	if err != nil {
		return nil, NewRawError(err, UncategorizedError)
	}
	ret := &LSMTree{
		memTable:     nil,
		ssTables:     make([]*SSTable, 0),
		dataLocation: dataLocation,
	}
	for _, e := range entries {
		if !isSStFile(e.Name()) {
			continue
		}
		fullPath := dataLocation + "/" + e.Name()
		sst, err := NewSsTableFromFile(fullPath)
		if err != nil {
			return nil, NewRawError(err, UncategorizedError)
		}
		ret.ssTables = append(ret.ssTables, sst)
	}
	return ret, NoError
}

type LSMTree struct {
	memTable *memTable
	ssTables []*SSTable

	tableMutex   sync.RWMutex
	dataLocation string
}

func (table *LSMTree) Mode() KVStoreMode {
	return ModeHybrid
}

func (table *LSMTree) FlushToDisk() DbError {
	table.tableMutex.Lock()
	defer table.tableMutex.Unlock()
	return table.flushToDiskAssumeLock()
}

func (table *LSMTree) flushToDiskAssumeLock() DbError {
	if table.memTable == nil {
		return NoError
	}
	flushLocation := fmt.Sprintf("%v/%v.sst", table.dataLocation, time.Now().UnixMicro())
	flushed, err := memtableToSSTable(flushLocation, table.memTable)
	if err.error != nil {
		return err
	}
	table.memTable = nil
	table.ssTables = append(table.ssTables, flushed)
	return NoError
}

func (table *LSMTree) getInMem() (*memTable, DbError) {
	if table.memTable != nil && !table.memTable.isFull() {
		return table.memTable, NoError
	}

	table.tableMutex.Lock()
	defer table.tableMutex.Unlock()
	if table.memTable != nil && !table.memTable.isFull() {
		return table.memTable, NoError
	}
	if err := table.flushToDiskAssumeLock(); err.error != nil {
		return nil, NoError
	}
	table.memTable = NewMemTable(defaultMemTableConfig)
	return table.memTable, NoError
}

func (table *LSMTree) Put(key, value []byte) DbError {
	if table, err := table.getInMem(); err.error != nil {
		return err
	} else {
		return table.Put(key, value)
	}

}

func (table *LSMTree) Delete(key []byte) DbError {
	if table, err := table.getInMem(); err.error != nil {
		return err
	} else {
		return table.Delete(key)
	}
}

func (table *LSMTree) Get(key []byte) (value []byte, err DbError) {
	if currentTable, err := table.getInMem(); err.error != nil {
		return nil, err
	} else if value, err = currentTable.Get(key); err.Success() || err.ErrorType == KeyMarkedAsDeleted {
		return value, err
	}
	for i := len(table.ssTables) - 1; i >= 0; i-- {
		currentTable := table.ssTables[i]
		value, err = currentTable.Get(key)
		if err.Success() || err.ErrorType == KeyMarkedAsDeleted {
			return value, err
		}
	}
	return nil, NewError("Not exists", KeyNotExists)
}

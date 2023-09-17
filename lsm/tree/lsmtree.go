package tree

import (
	"fmt"
	"sync"
	"time"
)

type LSMTree struct {
	memTable *memTable
	ssTables []*SSTable

	tableMutex   sync.RWMutex
	dataLocation string
}

func (table *LSMTree) Mode() KVStoreMode {
	return ModeHybrid
}

func (table *LSMTree) flushToDisk() DbError {
	if table.memTable == nil {
		return NoError
	}
	flushLocation := fmt.Sprintf("%v/sstable_%v", table.dataLocation, time.Now().UnixMicro())
	flushed, err := memtableToSSTable(flushLocation, table.memTable)
	if err.error != nil {
		return err
	}
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
	if err := table.flushToDisk(); err.error != nil {
		return nil, NoError
	}
	table.memTable = NewMemTable(defaultMemTableConfig)
	return table.memTable, NoError
}

func (table *LSMTree) Put(key, value string) DbError {
	if table, err := table.getInMem(); err.error != nil {
		return err
	} else {
		return table.Put(key, value)
	}

}

func (table *LSMTree) Delete(key string) DbError {
	if table, err := table.getInMem(); err.error != nil {
		return err
	} else {
		return table.Delete(key)
	}
}

func (table *LSMTree) Get(key string) (value string, err DbError) {
	if currentTable, err := table.getInMem(); err.error != nil {
		return "", err
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
	return "", NewError("Not exists", KeyNotExists)
}

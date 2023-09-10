package tree

import (
	"fmt"
	"sync"
	"time"
)

type LSMTree struct {
	// XXX: memtable should just be a btree or skip list and not sstable.SSTable
	memtable   *SSTable
	diskTables []*SSTable

	tableMutex   sync.RWMutex
	dataLocation string
}

func (table *LSMTree) Mode() KVStoreMode {
	return ModeHybrid
}

func (table *LSMTree) getInMem() *SSTable {
	if table.memtable != nil && table.memtable.Mode() == ModeInMem {
		return table.memtable
	}

	table.tableMutex.Lock()
	defer table.tableMutex.Unlock()
	if table.memtable != nil && table.memtable.Mode() == ModeInMem {
		return table.memtable
	}
	if table.memtable != nil && table.memtable.Mode() != ModeInMem {
		table.diskTables = append(table.diskTables, table.memtable)
		table.memtable = nil
	}
	flushLocation := fmt.Sprintf("%v/sstable_%v", table.dataLocation, time.Now().UnixMicro())
	memtable := EmptyWithDefaultConfig(flushLocation)
	table.memtable = &memtable
	return table.memtable
}

func (table *LSMTree) Put(key, value string) DbError {
	return table.getInMem().Put(key, value)
}

func (table *LSMTree) Delete(key string) DbError {
	return table.getInMem().Delete(key)
}

func (table *LSMTree) Get(key string) (value string, err DbError) {
	currentTable := table.getInMem()

	value, err = currentTable.Get(key)
	if err.Success() || err.ErrorType == KeyMarkedAsDeleted {
		return value, err
	}
	for i := len(table.diskTables) - 1; i >= 0; i-- {
		currentTable = table.diskTables[i]
		value, err = currentTable.Get(key)
		if err.Success() || err.ErrorType == KeyMarkedAsDeleted {
			return value, err
		}
	}
	return "", NewError("Not exists", KeyNotExists)
}

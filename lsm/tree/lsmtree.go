package tree

import (
	"fmt"
	"sync"
	"time"
)

type LSMTree struct {
	// XXX: memtable should just be a btree or skip list and not sstable.SSTable
	memtable   *memTable
	diskTables []*SSTable

	tableMutex   sync.RWMutex
	dataLocation string
}

func (table *LSMTree) Mode() KVStoreMode {
	return ModeHybrid
}

func (table *LSMTree) flushToDisk() DbError {
	if table.memtable == nil {
		return NoError
	}
	flushLocation := fmt.Sprintf("%v/sstable_%v", table.dataLocation, time.Now().UnixMicro())
	flushed, err := memtableToSSTable(flushLocation, table.memtable)
	if err.error != nil {
		return err
	}
	table.diskTables = append(table.diskTables, flushed)
	return NoError
}

func (table *LSMTree) getInMem() (*memTable, DbError) {
	if table.memtable != nil && !table.memtable.isFull() {
		return table.memtable, NoError
	}

	table.tableMutex.Lock()
	defer table.tableMutex.Unlock()
	if table.memtable != nil && !table.memtable.isFull() {
		return table.memtable, NoError
	}
	if err := table.flushToDisk(); err.error != nil {
		return nil, NoError
	}
	table.memtable = NewMemTable(defaultMemTableConfig)
	return table.memtable, NoError
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
	for i := len(table.diskTables) - 1; i >= 0; i-- {
		currentTable := table.diskTables[i]
		value, err = currentTable.Get(key)
		if err.Success() || err.ErrorType == KeyMarkedAsDeleted {
			return value, err
		}
	}
	return "", NewError("Not exists", KeyNotExists)
}

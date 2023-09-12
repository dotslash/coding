package tree

import (
	"fmt"
	"github.com/google/btree"
	"os"
	"yesteapea.com/lsm/filter"
)

func defaultFilter() *filter.BitsAndBloomsFilter {
	return &filter.BitsAndBloomsFilter{}
}

func newROFilterFromMem(mem *btree.BTreeG[ssTableItem]) (filter.Filter, error) {
	ret := defaultFilter()
	ret.Init(uint(mem.Len()), 0.01)
	var err error
	handle := func(item ssTableItem) bool {
		if !ret.Insert(item.key) {
			err = fmt.Errorf("failed to insert %v", item.key)
			return false
		}
		return true
	}
	mem.Ascend(handle)
	if err != nil {
		return nil, err
	} else {
		ret.Freeze()
		return ret, nil
	}
}

func saveFilterToFile(filename string, f filter.Filter) error {
	data, err := f.Encode()
	if err != nil {
		return err
	} else if err = os.WriteFile(filename, data, 0644); err != nil {
		return err
	}
	return nil
}

func loadROFilterFromFile(filename string) (filter.Filter, error) {
	ret := defaultFilter()
	if data, err := os.ReadFile(filename); err != nil {
		return nil, err
	} else if _, err = ret.Decode(data); err != nil {
		return nil, err
	} else {
		ret.Freeze()
		return ret, nil
	}
}

package sstable

import (
	"encoding/binary"
	"errors"
	"github.com/google/btree"
	"os"
)

const (
	ModeInMem  = 1
	ModeInFile = 2
)

const DefaultMaxMemTableSize = int64(32 * 1024 * 1024)
const DefaultSparseIndexMaxGap = int64(128 * 1024)

type ssTableFooterInfo struct {
	numDataBytes        int64
	numSparseIndexBytes int64
}

type Config struct {
	MaxMemTableSize      int64
	MaxSparseIndexMapGap int64
}

type SSTable struct {
	// Either the memTable will be present or the fileName will be present
	// If the SS table is in file Mode, then sparseIndex is lazily created
	// When its in file Mode, ss table is immutable i.e write operations will
	// be disallowed
	// TODO: read write locks
	memTable     *btree.BTree
	memTableSize int

	sparseIndex *btree.BTreeG[sparseIndexItem]
	fileName    string
	footer      *ssTableFooterInfo
	config      Config
}

type sparseIndexItem struct {
	key    string
	offset int64
}

func sparseIndexItemLess(i1, i2 sparseIndexItem) bool {
	return i1.key < i2.key
}

type ssTableItem struct {
	key       string
	value     string
	isDeleted bool
}

func (item *ssTableItem) size() int {
	return len(item.key) + len(item.value) + 1

}

func (item ssTableItem) Less(other btree.Item) bool {
	castedOther, ok := other.(ssTableItem)
	if !ok {
		// ssTableItem is always >= other btree.Item
		return false
	}
	return item.key < castedOther.key
}

func NewFromFile(name string) SSTable {
	return SSTable{fileName: name}
}

func EmptyWithDefaultConfig(fileName string) SSTable {
	return SSTable{
		memTable: btree.New(2),
		fileName: fileName,
		config: Config{
			MaxMemTableSize:      DefaultMaxMemTableSize,
			MaxSparseIndexMapGap: DefaultSparseIndexMaxGap,
		},
	}
}

func Empty(fileName string, config Config) SSTable {
	return SSTable{
		memTable: btree.New(2),
		fileName: fileName,
		config:   config,
	}
}

func (table *SSTable) Mode() int32 {
	if table.memTable != nil {
		return ModeInMem
	} else {
		return ModeInFile
	}
}

func (table *SSTable) putInternal(item ssTableItem) error {
	if table.Mode() != ModeInMem {
		return errors.New("RO SSTtable")
	}
	oldRaw := table.memTable.ReplaceOrInsert(item)
	if oldRaw == nil {
		table.memTableSize += item.size()
	} else {
		old := oldRaw.(ssTableItem)
		table.memTableSize += item.size() - old.size()
	}
	if int64(table.memTableSize) > table.config.MaxMemTableSize {
		return table.ConvertToSegmentFile()
	}
	return nil
}

func (table *SSTable) Put(key, value string) error {
	return table.putInternal(ssTableItem{key: key, value: value})
}

func (table *SSTable) Delete(key string) error {
	return table.putInternal(ssTableItem{key: key, isDeleted: true})
}

func (table *SSTable) setupFooter(f *os.File) (err error) {
	if table.footer != nil {
		return nil
	}
	// TODO: Handle concurrency here
	b8 := make([]byte, 8)
	if _, err = f.Seek(-16, 2); err != nil {
		return err
	}
	if _, err = f.Read(b8); err != nil {
		return err
	}
	numDataBytes := int64(binary.BigEndian.Uint64(b8))

	if _, err = f.Read(b8); err != nil {
		return err
	}
	numSparseIndexBytes := int64(binary.BigEndian.Uint64(b8))
	table.footer = &ssTableFooterInfo{
		numDataBytes:        numDataBytes,
		numSparseIndexBytes: numSparseIndexBytes,
	}
	return err
}

func (table *SSTable) ensureSparseIndex() error {
	if table.sparseIndex != nil {
		return nil
	}
	// TODO: Add mutex to ensure this runs only once
	f, err := os.Open(table.fileName)
	if err != nil {
		return err
	}
	// Seek to the start of sparseIndex
	if err = table.setupFooter(f); err != nil {
		return err
	}
	sparseIndexStart, sparseIndexEnd :=
		table.footer.numDataBytes,
		table.footer.numDataBytes+table.footer.numSparseIndexBytes
	curIndex := sparseIndexStart
	sparseIndex := btree.NewG(2, sparseIndexItemLess)
	for curIndex < sparseIndexEnd {
		var item sparseIndexItem
		var err error
		item, curIndex, err = table.nextSparseIndexEntry(f, curIndex)
		if err != nil {
			return err
		}
		sparseIndex.ReplaceOrInsert(item)
	}
	table.sparseIndex = sparseIndex
	return nil
}

// TODO: return error value as well.
func (table *SSTable) getFromDisk(key string) (value string, ok bool) {
	offset := int64(-1)
	handle := func(item sparseIndexItem) bool {
		offset = item.offset
		return false // We dont need to iterate. We want offset of the largest entry <= key
	}
	if err := table.ensureSparseIndex(); err != nil {
		return err.Error(), false
	}
	table.sparseIndex.DescendLessOrEqual(sparseIndexItem{key: key}, handle)
	if offset < 0 {
		// key is smaller than the smallest entry of the table.
		return "", false
	}
	f, err := os.Open(table.fileName)
	if err != nil {
		return err.Error(), false
	}
	defer f.Close()
	var item ssTableItem
	for offset < table.footer.numDataBytes {
		if item, offset, err = table.nextDiskEntry(f, offset); err != nil {
			return err.Error(), false
		} else if item.key == key {
			// match
			if item.isDeleted {
				return "Marked as deleted", false
			} else {
				return item.value, true
			}
		} else if item.key > key {
			// We are past the current key
			return "", false
		}
	}
	return "End of DataSection", false
}

func (table *SSTable) Get(key string) (value string, ok bool) {
	if table.Mode() != ModeInMem {
		return table.getFromDisk(key)
	}
	rawExisting := table.memTable.Get(ssTableItem{key: key})
	if rawExisting == nil {
		return "", false
	} else if existing := rawExisting.(ssTableItem); existing.isDeleted {
		return "", false
	} else {
		return existing.value, true
	}
}

func (table *SSTable) ConvertToSegmentFile() error {
	if table.Mode() == ModeInFile {
		// XXX: Return an error?
		return nil
	}
	f, err := os.Create(table.fileName)
	if err != nil {
		return err
	}
	defer func() {
		err2 := f.Close()
		if err == nil {
			err = err2
		}
	}()

	b4 := make([]byte, 4)
	b8 := make([]byte, 8)
	var n1, n2, n3, n4 int

	// Write the data section and fill in-memory sparse index
	numDataBytes := int64(0)
	lastSparseIndexUpdateOffset := int64(-1)
	table.sparseIndex = btree.NewG(2, sparseIndexItemLess)
	memTableHandle := func(itemRaw btree.Item) bool {
		item := itemRaw.(ssTableItem)
		binary.BigEndian.PutUint32(b4, uint32(len(item.key)))
		// XXX: Handle the errors here correctly
		n1, err = f.Write(b4)
		n2, err = f.Write([]byte(item.key))
		valLengthAndIsDeleted := uint32(len(item.value))
		if item.isDeleted {
			valLengthAndIsDeleted = 1 << 31
		}
		binary.BigEndian.PutUint32(b4, valLengthAndIsDeleted)
		n3, err = f.Write(b4)
		n4, err = f.Write([]byte(item.value))
		oldNumDataBytes := numDataBytes
		numDataBytes += int64(n1 + n2 + n3 + n4)
		if lastSparseIndexUpdateOffset < 0 ||
			numDataBytes >= table.config.MaxSparseIndexMapGap+lastSparseIndexUpdateOffset {
			table.sparseIndex.ReplaceOrInsert(sparseIndexItem{key: item.key, offset: oldNumDataBytes})
			lastSparseIndexUpdateOffset = oldNumDataBytes
		}
		return true
	}
	table.memTable.Ascend(memTableHandle)
	// Persist the sparse index on disk
	numSparseIndexBytes := int64(0)
	sparseIndexHandle := func(item sparseIndexItem) bool {
		// XXX: Handle the errors here correctly
		// Length of key
		binary.BigEndian.PutUint32(b4, uint32(len(item.key)))
		n1, err = f.Write(b4)
		// key bytes
		n2, err = f.Write([]byte(item.key))
		// offset in data section
		binary.BigEndian.PutUint64(b8, uint64(item.offset))
		n3, err = f.Write(b8)
		// Update num bytes written
		numSparseIndexBytes += int64(n1 + n2 + n3)
		return true
	}
	table.sparseIndex.Ascend(sparseIndexHandle)
	// Footer
	binary.BigEndian.PutUint64(b8, uint64(numDataBytes))
	if n1, err = f.Write(b8); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(b8, uint64(numSparseIndexBytes))
	if n2, err = f.Write(b8); err != nil {
		return err
	}
	table.memTable = nil
	if err = f.Close(); err != nil {
		f = nil
		return err
	}
	table.footer = &ssTableFooterInfo{
		numDataBytes:        numDataBytes,
		numSparseIndexBytes: numSparseIndexBytes,
	}
	f = nil
	// f.close done by defer
	return err
}

func (table *SSTable) nextSparseIndexEntry(f *os.File, offset int64) (item sparseIndexItem, newOffSet int64, err error) {
	newOffSet = offset
	bytesRead := 0
	if _, err := f.Seek(offset, 0); err != nil {
		return sparseIndexItem{}, 0, err
	}
	b8 := make([]byte, 8)
	b4 := make([]byte, 4)
	if bytesRead, err = f.Read(b4); err != nil {
		return sparseIndexItem{}, 0, err
	}
	keyLen := binary.BigEndian.Uint32(b4)
	newOffSet += int64(bytesRead)

	keyBytes := make([]byte, keyLen)
	if bytesRead, err = f.Read(keyBytes); err != nil {
		return sparseIndexItem{}, 0, err
	}
	item.key = string(keyBytes)
	newOffSet += int64(bytesRead)

	if bytesRead, err = f.Read(b8); err != nil {
		return sparseIndexItem{}, 0, err
	}
	item.offset = int64(binary.BigEndian.Uint64(b8))
	newOffSet += int64(bytesRead)
	return item, newOffSet, nil
}

func (table *SSTable) nextDiskEntry(f *os.File, offset int64) (item ssTableItem, newOffSet int64, err error) {
	newOffSet = offset
	bytesRead := 0
	if _, err := f.Seek(offset, 0); err != nil {
		return ssTableItem{}, 0, err
	}

	b4 := make([]byte, 4)
	if bytesRead, err = f.Read(b4); err != nil {
		return ssTableItem{}, 0, err
	}
	keyLen := binary.BigEndian.Uint32(b4)
	newOffSet += int64(bytesRead)

	keyBytes := make([]byte, keyLen)
	if bytesRead, err = f.Read(keyBytes); err != nil {
		return ssTableItem{}, 0, err
	}
	item.key = string(keyBytes)
	newOffSet += int64(bytesRead)

	if bytesRead, err = f.Read(b4); err != nil {
		return ssTableItem{}, 0, err
	}
	valueLen := binary.BigEndian.Uint32(b4)
	newOffSet += int64(bytesRead)

	if valueLen&uint32(1<<31) != 0 {
		item.isDeleted = true
	} else {
		valueBytes := make([]byte, valueLen)
		if bytesRead, err = f.Read(valueBytes); err != nil {
			return ssTableItem{}, 0, err
		}
		item.value = string(valueBytes)
		newOffSet += int64(bytesRead)
	}
	return item, newOffSet, nil
}

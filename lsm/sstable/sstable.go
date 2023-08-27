package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/google/btree"
	"io"
	"os"
)

const (
	ModeInMem  = 1
	ModeInFile = 2
)

const DefaultMaxMemTableSize = int64(32 * 1024 * 1024)
const DefaultSparseIndexMaxGap = int64(128 * 1024)

type bufferWithSeek struct {
	*bufio.Reader
	raw io.ReadWriteSeeker
}

func (s *bufferWithSeek) Seek(offset int64, whence int) (newOffset int64, err error) {
	// Perform the raw seek
	newOffset, err = s.raw.Seek(offset, whence)
	if err != nil {
		return 0, err
	}

	// Reset the buffer state
	s.Reset(s.raw)
	return newOffset, nil
}

func newBufferWithSeek(f *os.File) *bufferWithSeek {
	ret := &bufferWithSeek{
		Reader: bufio.NewReader(f),
		raw:    f,
	}
	return ret
}

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

func (table *SSTable) setupFooter(f *bufferWithSeek) (err error) {
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
	rawf, err := os.Open(table.fileName)
	f := newBufferWithSeek(rawf)
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
	if _, err = f.Seek(curIndex, io.SeekStart); err != nil {
		return err
	}
	for curIndex < sparseIndexEnd {
		item, nBytesRead, err := table.nextSparseIndexEntry(f)
		if err != nil {
			return err
		}
		curIndex += int64(nBytesRead)
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
	rawf, err := os.Open(table.fileName)
	f := newBufferWithSeek(rawf)
	if err != nil {
		return err.Error(), false
	}
	defer rawf.Close()
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return err.Error(), false
	}
	for offset < table.footer.numDataBytes {
		if item, nBytesRead, err := table.nextDiskEntry(f); err != nil {
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
			return item.key, false
		} else {
			offset += int64(nBytesRead)
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
	fRaw, err := os.Create(table.fileName)
	if err != nil {
		return err
	}
	f := bufio.NewWriter(fRaw)
	defer func() {
		err2 := fRaw.Close()
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
	if err = fRaw.Close(); err != nil {
		fRaw = nil
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

func (table *SSTable) nextSparseIndexEntry(f *bufferWithSeek) (item sparseIndexItem, bytesRead int, err error) {
	var bytesReadCur int
	bytesRead = 0
	b8 := make([]byte, 8)
	b4 := make([]byte, 4)
	if bytesReadCur, err = f.Read(b4); err != nil {
		return sparseIndexItem{}, bytesReadCur + bytesRead, err
	}
	bytesRead += bytesReadCur
	keyLen := binary.BigEndian.Uint32(b4)

	keyBytes := make([]byte, keyLen)
	if bytesReadCur, err = f.Read(keyBytes); err != nil {
		return sparseIndexItem{}, bytesReadCur + bytesRead, err
	}
	bytesRead += bytesReadCur
	item.key = string(keyBytes)

	if bytesReadCur, err = f.Read(b8); err != nil {
		return sparseIndexItem{}, bytesReadCur + bytesRead, err
	}
	bytesRead += bytesReadCur
	item.offset = int64(binary.BigEndian.Uint64(b8))
	return item, bytesRead, nil
}

func (table *SSTable) nextDiskEntry(f *bufferWithSeek) (item ssTableItem, nBytesRead int, err error) {
	nBytesReadCur := 0
	nBytesRead = 0
	b4 := make([]byte, 4)
	if nBytesReadCur, err = io.ReadFull(f, b4); err != nil {
		return ssTableItem{}, nBytesReadCur + nBytesRead, err
	}
	nBytesRead += nBytesReadCur
	keyLen := binary.BigEndian.Uint32(b4)

	keyBytes := make([]byte, keyLen)
	if nBytesReadCur, err = io.ReadFull(f, keyBytes); err != nil {
		return ssTableItem{}, nBytesReadCur + nBytesRead, err
	}
	nBytesRead += nBytesReadCur
	item.key = string(keyBytes)

	if nBytesReadCur, err = io.ReadFull(f, b4); err != nil {
		return ssTableItem{}, nBytesReadCur + nBytesRead, err
	}
	nBytesRead += nBytesReadCur
	valueLen := binary.BigEndian.Uint32(b4)

	if valueLen&uint32(1<<31) != 0 {
		item.isDeleted = true
	} else {
		valueBytes := make([]byte, valueLen)
		if nBytesReadCur, err = io.ReadFull(f, valueBytes); err != nil {
			return ssTableItem{}, nBytesReadCur + nBytesRead, err
		}
		nBytesRead += nBytesReadCur
		item.value = string(valueBytes)
	}
	return item, nBytesRead, nil
}

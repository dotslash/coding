package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/google/btree"
	"io"
	"math/rand"
	"os"
	"sync"
	"yesteapea.com/lsm/tree"
)

const (
	ModeInMem  = 1
	ModeInFile = 2
)

const DefaultMaxMemTableSize = int64(32 * 1024 * 1024)
const DefaultSparseIndexMaxGap = int64(128 * 1024)

const (
	errNotExists                 = "Not exists"
	errNotExistsMarkedAsDeleted  = "Marked as deleted"
	errNotExistsEndOfDataSection = "end of DataSection"
	errNotExistsPastCurrentKey   = "past the current key"
)

// === bufReaderWithSeek start

type bufReaderWithSeek struct {
	*bufio.Reader
	raw  io.ReadWriteSeeker
	lock sync.Locker
}

func (s *bufReaderWithSeek) Seek(offset int64, whence int) (newOffset int64, err error) {
	// Perform the raw seek
	newOffset, err = s.raw.Seek(offset, whence)
	if err != nil {
		return 0, err
	}

	// Reset the buffer state
	s.Reset(s.raw)
	return newOffset, nil
}

// === bufReaderWithSeek end
// === sparseIndexItem begin

type sparseIndexItem struct {
	key    string
	offset int64
}

func sparseIndexItemLess(i1, i2 sparseIndexItem) bool {
	return i1.key < i2.key
}

// === sparseIndexItem end
// === ssTableItem begin

type ssTableItem struct {
	key       []byte
	value     []byte
	isDeleted bool
}

func (item *ssTableItem) size() int {
	return len(item.key) + len(item.value) + 1
}

func ssTableItemLess(i1, i2 ssTableItem) bool {
	return bytes.Compare(i1.key, i2.key) < 0
}

// === ssTableItem end
// === ssTableItemDisk begin

type ssTableItemDisk struct {
	key       []byte
	value     []byte
	keyLen    int
	valueLen  int
	isDeleted bool
}

func (item *ssTableItemDisk) getKeySlice() []byte {
	return item.key[:item.keyLen]
}

func (item *ssTableItemDisk) getValueSlice() []byte {
	return item.value[:item.valueLen]
}

func (item *ssTableItemDisk) setKeyLen(keyLen int) {
	item.keyLen = keyLen
	if len(item.key) < item.keyLen {
		item.value = make([]byte, item.keyLen)
	}
}

func (item *ssTableItemDisk) setValueLen(valueLen int) {
	item.valueLen = valueLen
	if len(item.value) < item.valueLen {
		item.value = make([]byte, item.valueLen)
	}
}

// === ssTableItemDisk end

type ssTableFooterInfo struct {
	numDataBytes        int64
	numSparseIndexBytes int64
}

type Config struct {
	MaxMemTableSize      int64
	MaxSparseIndexMapGap int64
}

// SSTable is either in ModeInFile or ModeInMem
//
// TODO: SStable is supposed to the on disk file + related in mem
// datastructures. The inmem mode here is supposed to be purely
// handled by lsmtree's memtable.
//
// In ModeInMem
//   - memTable will be used for reads and writes
//   - memTable size is tracked via memTableSize and if it goes beyond config.MaxMemTableSize then
//     it will be converted to ModeInFile
//
// In ModeInFile
//   - sparseIndex is lazily created.
//   - SSTable is immutable i.e write operations will be disallowed
type SSTable struct {
	// TODO: read write locks

	// ModeInMem related data structures
	memTable     *btree.BTreeG[ssTableItem]
	memTableSize int

	// ModeInFile related data structures
	sparseIndex *btree.BTreeG[sparseIndexItem]
	fileName    string
	footer      *ssTableFooterInfo
	readers     []*bufReaderWithSeek

	// General items
	config     Config
	tableMutex sync.RWMutex
}

func createEmptyReaders(cnt int) []*bufReaderWithSeek {
	ret := make([]*bufReaderWithSeek, cnt)
	for i := 0; i < cnt; i++ {
		ret[i] = &bufReaderWithSeek{lock: &sync.Mutex{}}
	}
	return ret
}

func NewFromFile(name string) SSTable {
	return SSTable{
		fileName: name,
		readers:  createEmptyReaders(16),
	}
}

func EmptyWithDefaultConfig(fileName string) SSTable {
	defaultConf := Config{MaxMemTableSize: DefaultMaxMemTableSize, MaxSparseIndexMapGap: DefaultSparseIndexMaxGap}
	return Empty(fileName, defaultConf)
}

func Empty(fileName string, config Config) SSTable {
	return SSTable{
		memTable: btree.NewG(2, ssTableItemLess),
		fileName: fileName,
		config:   config,
		readers:  createEmptyReaders(16),
	}
}

func (table *SSTable) Mode() int32 {
	if table.memTable != nil {
		return ModeInMem
	} else {
		return ModeInFile
	}
}

func (table *SSTable) putInternal(item ssTableItem) tree.DbError {
	if table.Mode() != ModeInMem {
		return tree.NewError("RO SSTtable", tree.ROTable)
	}
	table.tableMutex.Lock()
	defer table.tableMutex.Unlock()
	old, oldExists := table.memTable.ReplaceOrInsert(item)
	if !oldExists {
		table.memTableSize += item.size()
	} else {
		table.memTableSize += item.size() - old.size()
	}
	if int64(table.memTableSize) > table.config.MaxMemTableSize {
		return table.ConvertToSegmentFile()
	}
	return tree.NoError
}

func (table *SSTable) Put(key, value string) tree.DbError {
	return table.putInternal(ssTableItem{key: []byte(key), value: []byte(value)})
}

func (table *SSTable) Delete(key string) tree.DbError {
	return table.putInternal(ssTableItem{key: []byte(key), isDeleted: true})
}

// setupFooter is expected by to called by ensureSparseIndex when
// tableMutex is held
func (table *SSTable) setupFooter(f *bufReaderWithSeek) (dberr tree.DbError) {
	if table.footer != nil {
		return tree.NoError
	}
	b8 := make([]byte, 8)
	if _, err := f.Seek(-16, io.SeekEnd); err != nil {
		return tree.NewInternalError(err)
	}
	if _, err := f.Read(b8); err != nil {
		return tree.NewInternalError(err)
	}
	numDataBytes := int64(binary.BigEndian.Uint64(b8))

	if _, err := f.Read(b8); err != nil {
		return tree.NewInternalError(err)
	}
	numSparseIndexBytes := int64(binary.BigEndian.Uint64(b8))
	table.footer = &ssTableFooterInfo{
		numDataBytes:        numDataBytes,
		numSparseIndexBytes: numSparseIndexBytes,
	}
	return tree.NoError
}

func (table *SSTable) ensureSparseIndex() tree.DbError {
	if table.sparseIndex != nil {
		return tree.NoError
	}
	table.tableMutex.Lock()
	defer table.tableMutex.Unlock()

	f, err := table.getFileHandle()
	defer f.lock.Unlock()
	if err.GetError() != nil {
		return err
	}
	// Seek to the start of sparseIndex
	if err = table.setupFooter(f); err.GetError() != nil {
		return err
	}
	sparseIndexStart, sparseIndexEnd :=
		table.footer.numDataBytes,
		table.footer.numDataBytes+table.footer.numSparseIndexBytes
	curIndex := sparseIndexStart
	sparseIndex := btree.NewG(2, sparseIndexItemLess)
	if _, ferr := f.Seek(curIndex, io.SeekStart); ferr != nil {
		return tree.NewInternalError(ferr)
	}
	for curIndex < sparseIndexEnd {
		item, nBytesRead, err := table.nextSparseIndexItemFromFile(f)
		if err.GetError() != nil {
			return err
		}
		curIndex += int64(nBytesRead)
		sparseIndex.ReplaceOrInsert(item)
	}
	table.sparseIndex = sparseIndex
	return tree.NoError
}

func (table *SSTable) getFileHandle() (ret *bufReaderWithSeek, dberr tree.DbError) {
	readerId := rand.Int() % len(table.readers)
	table.readers[readerId].lock.Lock()
	if table.readers[readerId].Reader == nil {
		if rawFile, err := os.Open(table.fileName); err != nil {
			return nil, tree.NewInternalError(err)
		} else {
			table.readers[readerId].Reader = bufio.NewReader(rawFile)
			table.readers[readerId].raw = rawFile
		}
	}
	return table.readers[readerId], tree.NoError
}

func (table *SSTable) getFromDisk(key string) (value string, dberr tree.DbError) {
	offset := int64(-1)
	handle := func(item sparseIndexItem) bool {
		offset = item.offset
		return false // We dont need to iterate. We want offset of the largest entry <= key
	}
	if err := table.ensureSparseIndex(); err.GetError() != nil {
		return "", err
	}
	table.sparseIndex.DescendLessOrEqual(sparseIndexItem{key: key}, handle)
	if offset < 0 {
		// key is smaller than the smallest entry of the table.
		return "", tree.NewError(errNotExists, tree.KeyNotExists)
	}
	f, err := table.getFileHandle()
	defer f.lock.Unlock()
	if err.GetError() != nil {
		return "", err
	}
	if _, ferr := f.Seek(offset, io.SeekStart); ferr != nil {
		return "", tree.NewInternalError(ferr)
	}
	keyBytes := []byte(key)
	item := ssTableItemDisk{
		key:       make([]byte, 128), // XXX: Should this be a config?
		value:     make([]byte, 128), // XXX: Should this be a config?
		keyLen:    0,
		valueLen:  0,
		isDeleted: false,
	}
	for offset < table.footer.numDataBytes {
		if nBytesRead, err := table.nextSStableItemFromFile(f, keyBytes, &item); err.GetError() != nil {
			return "", err
		} else if cmp := bytes.Compare(item.getKeySlice(), keyBytes); cmp == 0 {
			// match
			if item.isDeleted {
				return "", tree.NewError(errNotExistsMarkedAsDeleted, tree.KeyNotExists)
			} else {
				return string(item.getValueSlice()), tree.NoError
			}
		} else if cmp > 0 {
			// We are past the current key
			return "", tree.NewError(errNotExistsPastCurrentKey, tree.KeyNotExists)
		} else {
			offset += int64(nBytesRead)
		}
	}
	return "", tree.NewError(errNotExistsEndOfDataSection, tree.KeyNotExists)
}

func (table *SSTable) Get(key string) (value string, dberr tree.DbError) {
	if table.Mode() != ModeInMem {
		return table.getFromDisk(key)
	}
	table.tableMutex.RLock()
	defer table.tableMutex.RUnlock()
	existing, ok := table.memTable.Get(ssTableItem{key: []byte(key)})
	if !ok {
		return "", tree.NewError(errNotExists, tree.KeyNotExists)
	} else if existing.isDeleted {
		return "", tree.NewError(errNotExistsMarkedAsDeleted, tree.KeyNotExists)
	} else {
		return string(existing.value), tree.NoError
	}
}

func (table *SSTable) ConvertToSegmentFile() tree.DbError {
	// TODO: Lock the datastructure here.
	if table.Mode() == ModeInFile {
		return tree.NoError
	}
	fRaw, err := os.Create(table.fileName)
	if err != nil {
		return tree.NewInternalError(err)
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
	memTableHandle := func(item ssTableItem) bool {
		binary.BigEndian.PutUint32(b4, uint32(len(item.key)))
		// XXX: Handle err here correctly
		n1, err = f.Write(b4)
		n2, err = f.Write(item.key)
		valLengthAndIsDeleted := uint32(len(item.value))
		if item.isDeleted {
			valLengthAndIsDeleted = 1 << 31
		}
		binary.BigEndian.PutUint32(b4, valLengthAndIsDeleted)
		n3, err = f.Write(b4)
		n4, err = f.Write(item.value)
		oldNumDataBytes := numDataBytes
		numDataBytes += int64(n1 + n2 + n3 + n4)
		if lastSparseIndexUpdateOffset < 0 ||
			numDataBytes >= table.config.MaxSparseIndexMapGap+lastSparseIndexUpdateOffset {
			table.sparseIndex.ReplaceOrInsert(sparseIndexItem{key: string(item.key), offset: oldNumDataBytes})
			lastSparseIndexUpdateOffset = oldNumDataBytes
		}
		return true
	}
	table.memTable.Ascend(memTableHandle)
	// Persist the sparse index on disk
	numSparseIndexBytes := int64(0)
	sparseIndexHandle := func(item sparseIndexItem) bool {
		// XXX: Handle err here correctly
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
		return tree.NewInternalError(err)
	}
	binary.BigEndian.PutUint64(b8, uint64(numSparseIndexBytes))
	if n2, err = f.Write(b8); err != nil {
		return tree.NewInternalError(err)
	}
	table.memTable = nil
	if err = fRaw.Close(); err != nil {
		fRaw = nil
		return tree.NewInternalError(err)
	}
	table.footer = &ssTableFooterInfo{
		numDataBytes:        numDataBytes,
		numSparseIndexBytes: numSparseIndexBytes,
	}
	f = nil
	// f.close done by defer
	return tree.NoError
}

func (table *SSTable) nextSparseIndexItemFromFile(
	f *bufReaderWithSeek,
) (item sparseIndexItem, bytesRead int, dberr tree.DbError) {
	var bytesReadCur int
	var err error
	bytesRead = 0
	b8 := make([]byte, 8)
	b4 := make([]byte, 4)
	if bytesReadCur, err = f.Read(b4); err != nil {
		return sparseIndexItem{}, bytesReadCur + bytesRead, tree.NewInternalError(err)
	}
	bytesRead += bytesReadCur
	keyLen := binary.BigEndian.Uint32(b4)

	keyBytes := make([]byte, keyLen)
	if bytesReadCur, err = f.Read(keyBytes); err != nil {
		return sparseIndexItem{}, bytesReadCur + bytesRead, tree.NewInternalError(err)
	}
	bytesRead += bytesReadCur
	item.key = string(keyBytes)

	if bytesReadCur, err = f.Read(b8); err != nil {
		return sparseIndexItem{}, bytesReadCur + bytesRead, tree.NewInternalError(err)
	}
	bytesRead += bytesReadCur
	item.offset = int64(binary.BigEndian.Uint64(b8))
	return item, bytesRead, tree.NoError
}

// nextSStableItemFromFile reads the next sstable item from disk. It returns the item value only if
// the key matches forKey. This will save some reads from disk.
func (table *SSTable) nextSStableItemFromFile(
	f *bufReaderWithSeek, forKey []byte, item *ssTableItemDisk,
) (nBytesRead int, dberr tree.DbError) {
	nBytesReadCur := 0
	nBytesRead = 0
	b4 := make([]byte, 4)
	var err error
	if nBytesReadCur, err = io.ReadFull(f, b4); err != nil {
		return nBytesReadCur + nBytesRead, tree.NewInternalError(err)
	}
	nBytesRead += nBytesReadCur
	item.setKeyLen(int(binary.BigEndian.Uint32(b4)))

	if nBytesReadCur, err = io.ReadFull(f, item.getKeySlice()); err != nil {
		return nBytesReadCur + nBytesRead, tree.NewInternalError(err)
	}
	nBytesRead += nBytesReadCur

	if nBytesReadCur, err = io.ReadFull(f, b4); err != nil {
		return nBytesReadCur + nBytesRead, tree.NewInternalError(err)
	}
	nBytesRead += nBytesReadCur
	valueLen := binary.BigEndian.Uint32(b4)

	if valueLen&uint32(1<<31) != 0 {
		item.isDeleted = true
	} else if bytes.Compare(item.key[:item.keyLen], forKey) == 0 {
		item.setValueLen(int(valueLen))
		if nBytesReadCur, err = io.ReadFull(f, item.getValueSlice()); err != nil {
			return nBytesReadCur + nBytesRead, tree.NewInternalError(err)
		}
		nBytesRead += nBytesReadCur
	} else {
		nBytesRead += int(valueLen)
		if _, err = f.Discard(int(valueLen)); err != nil {
			return 0, tree.NewInternalError(err)
		}
	}
	return nBytesRead, tree.NoError
}

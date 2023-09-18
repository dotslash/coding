package tree

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/btree"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"yesteapea.com/lsm/filter"
)

const (
	sstableErrNotExists                 = "not exists"
	sstableErrNotExistsFilter           = "not exists: Filter"
	sstableErrNotExistsMarkedAsDeleted  = "Marked as deleted"
	sstableErrNotExistsEndOfDataSection = "end of DataSection"
	sstableErrNotExistsPastCurrentKey   = "past the current key"
)

// === bufReaderWithSeek start

// bufReaderWithSeek is a custom implementation of ReadWriteSeeker with the following functionality
//   - Read(p []byte) will read into p fully until it hits error (including eof)
//   - It buffers data via bufio.Reader. Generally seek would not work nicely with buffering. So we override the
//     seek method and discard all buffer data whenever Seek is invoked
//   - It is not thread safe, like most other io utils. The user is expected to hold locks sensibly. For
//     convenience the struct also contains the lock. But locking is not enforced by bufReaderWithSeek
type bufReaderWithSeek struct {
	*bufio.Reader
	raw io.ReadWriteSeeker
	// The reader is not thread safe by itself. User of this util must a hold this lock appropriately.
	lock sync.Locker
}

// Generally, the semantic of Read is that, it will read at most len(p) bytes. It is the responsibility
// of the user to read more bytes if the input array does not have enough bytes. We can either fix all
// usages to ReadFull or conveniently override the Read method to the "right thing" - Choose the latter
func (s *bufReaderWithSeek) Read(p []byte) (n int, err error) {
	return io.ReadFull(s.Reader, p)
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

// SSTable is an immutable structure backed by data on disk. It is thread-safe.
type SSTable struct {
	sparseIndex *btree.BTreeG[sparseIndexItem]
	fileName    string
	footer      *ssTableFooterInfo
	readers     []*bufReaderWithSeek
	// filter will be used a cheap check to determine if a key is present in a sstable
	// It will have some false positives. But it must never have false negatives
	// Bloom filter/ cuckoo filter would work here.
	// filter will be used when the SSTable is file and is immutable. So the filter also will be
	// immutable. We still need a filterLock because the current implementation is not thread safe
	// for reads.
	filter filter.Filter

	// General items
	tableMutex sync.RWMutex
}

func createEmptyReaders(cnt int) []*bufReaderWithSeek {
	ret := make([]*bufReaderWithSeek, cnt)
	for i := 0; i < cnt; i++ {
		ret[i] = &bufReaderWithSeek{lock: &sync.Mutex{}}
	}
	return ret
}

func isSStFile(name string) bool {
	return strings.HasSuffix(name, ".sst")
}

func filterFileName(filename string) string {
	if !isSStFile(filename) {
		err := fmt.Errorf("got invalid sst file for filterFilename:%v", filename)
		panic(err)
	}
	prefix := filename[:len(filename)-4] // Remove .sst
	return prefix + ".filter"
}

func NewSsTableFromFile(name string) (*SSTable, error) {
	if !isSStFile(name) {
		return nil, errors.New("wrong suffix for sst file")
	}
	ret := SSTable{
		fileName: name,
		readers:  createEmptyReaders(16),
	}
	var err error
	var dbErr DbError
	ret.filter, err = loadROFilterFromFile(filterFileName(name))
	if err != nil {
		return nil, err
	} else if dbErr = ret.ensureSparseIndex(); dbErr.error != nil {
		return nil, dbErr.error
	} else {
		return &ret, nil
	}

}

func (table *SSTable) Mode() KVStoreMode {
	return ModeInFile
}

func (table *SSTable) Put(_, _ []byte) DbError {
	return NewError("RO SSTtable", ROTable)
}

func (table *SSTable) Delete(_ []byte) DbError {
	return NewError("RO SSTtable", ROTable)
}

// parseFooter is expected by to called by ensureSparseIndex when
// tableMutex is held
func parseFooter(f *bufReaderWithSeek) (footer *ssTableFooterInfo, dberr DbError) {
	b8 := make([]byte, 8)
	if _, err := f.Seek(-16, io.SeekEnd); err != nil {
		return nil, NewInternalError(err)
	}
	if _, err := f.Read(b8); err != nil {
		return nil, NewInternalError(err)
	}
	numDataBytes := int64(binary.BigEndian.Uint64(b8))

	if _, err := f.Read(b8); err != nil {
		return nil, NewInternalError(err)
	}
	numSparseIndexBytes := int64(binary.BigEndian.Uint64(b8))
	footer = &ssTableFooterInfo{
		numDataBytes:        numDataBytes,
		numSparseIndexBytes: numSparseIndexBytes,
	}
	return footer, NoError
}

func (table *SSTable) ensureSparseIndex() DbError {
	if table.sparseIndex != nil {
		return NoError
	}
	table.tableMutex.Lock()
	defer table.tableMutex.Unlock()

	f, err := table.getFileHandleAndLock()
	defer f.lock.Unlock()
	if err.GetError() != nil {
		return err
	}
	// Seek to the start of sparseIndex
	if table.footer, err = parseFooter(f); err.GetError() != nil {
		return err
	}
	sparseIndexStart, sparseIndexEnd :=
		table.footer.numDataBytes,
		table.footer.numDataBytes+table.footer.numSparseIndexBytes
	curIndex := sparseIndexStart
	sparseIndex := btree.NewG(2, sparseIndexItemLess)
	if _, ferr := f.Seek(curIndex, io.SeekStart); ferr != nil {
		return NewInternalError(ferr)
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
	return NoError
}

func (table *SSTable) getFileHandleAndLock() (ret *bufReaderWithSeek, dberr DbError) {
	readerId := rand.Int() % len(table.readers)
	table.readers[readerId].lock.Lock()
	if table.readers[readerId].Reader == nil {
		if rawFile, err := os.Open(table.fileName); err != nil {
			return nil, NewInternalError(err)
		} else {
			table.readers[readerId].Reader = bufio.NewReader(rawFile)
			table.readers[readerId].raw = rawFile
		}
	}
	return table.readers[readerId], NoError
}

func (table *SSTable) Get(key []byte) (value []byte, dberr DbError) {
	test := table.filter.Lookup(key)
	if !test {
		return nil, NewError(sstableErrNotExistsFilter, KeyNotExists)
	}

	offset := int64(-1)
	handle := func(item sparseIndexItem) bool {
		offset = item.offset
		return false // We dont need to iterate. We want offset of the largest entry <= key
	}
	if err := table.ensureSparseIndex(); err.GetError() != nil {
		return nil, err
	}
	table.sparseIndex.DescendLessOrEqual(sparseIndexItem{key: string(key)}, handle)
	if offset < 0 {
		// key is smaller than the smallest entry of the table.
		return nil, NewError(sstableErrNotExists, KeyNotExists)
	}
	f, err := table.getFileHandleAndLock()
	defer f.lock.Unlock()
	if err.GetError() != nil {
		return nil, err
	}
	if _, ferr := f.Seek(offset, io.SeekStart); ferr != nil {
		return nil, NewInternalError(ferr)
	}
	item := ssTableItemDisk{
		key:       make([]byte, 128), // XXX: Should this be a config?
		value:     make([]byte, 128), // XXX: Should this be a config?
		keyLen:    0,
		valueLen:  0,
		isDeleted: false,
	}
	for offset < table.footer.numDataBytes {
		if nBytesRead, err := table.nextSStableItemFromFile(f, key, &item); err.GetError() != nil {
			return nil, err
		} else if cmp := bytes.Compare(item.getKeySlice(), key); cmp == 0 {
			// match
			if item.isDeleted {
				return nil, NewError(sstableErrNotExistsMarkedAsDeleted, KeyMarkedAsDeleted)
			} else {
				return item.getValueSlice(), NoError
			}
		} else if cmp > 0 {
			// We are past the current key
			return nil, NewError(sstableErrNotExistsPastCurrentKey, KeyNotExists)
		} else {
			offset += int64(nBytesRead)
		}
	}
	return nil, NewError(sstableErrNotExistsEndOfDataSection, KeyNotExists)
}

func pickFirstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func closeWriter(b *bufio.Writer, f *os.File) (err error) {
	var err1, err2 error
	if b != nil {
		err1 = b.Flush()
	}
	if f != nil {
		err2 = f.Close()
	}
	return pickFirstErr(err1, err2)
}

func memtableToSSTable(filename string, memtable *memTable) (sst *SSTable, retErr DbError) {
	if !isSStFile(filename) {
		return nil, NewError("wrong suffix for sst file", UserError)
	}
	var err error
	// Create filter filter
	var filter filter.Filter
	filterFileName := filterFileName(filename)
	if filter, err = newROFilterFromMem(memtable.data); err != nil {
		return nil, NewInternalError(err)
	} else if err = saveFilterToFile(filterFileName, filter); err != nil {
		return nil, NewInternalError(err)
	}

	// Actual ss table
	fRaw, err := os.Create(filename)
	if err != nil {
		return nil, NewInternalError(err)
	}
	f := bufio.NewWriter(fRaw)
	defer func() {
		err := closeWriter(f, fRaw)
		if err != nil {
			retErr = NewInternalError(err)
		}
	}()

	b4 := make([]byte, 4)
	b8 := make([]byte, 8)
	var n1, n2, n3, n4 int

	// Write the data section and fill in-memory sparse index
	numDataBytes := int64(0)
	lastSparseIndexUpdateOffset := int64(-1)
	sparseIndex := btree.NewG(2, sparseIndexItemLess)
	var err1, err2, err3, err4 error
	memTableHandle := func(item memTableItem) bool {
		binary.BigEndian.PutUint32(b4, uint32(len(item.key)))
		n1, err1 = f.Write(b4)
		n2, err2 = f.Write(item.key)
		valLengthAndIsDeleted := uint32(len(item.value))
		if item.isDeleted {
			valLengthAndIsDeleted = 1 << 31
		}
		binary.BigEndian.PutUint32(b4, valLengthAndIsDeleted)
		n3, err3 = f.Write(b4)
		n4, err4 = f.Write(item.value)
		oldNumDataBytes := numDataBytes
		numDataBytes += int64(n1 + n2 + n3 + n4)
		if lastSparseIndexUpdateOffset < 0 ||
			numDataBytes >= memtable.config.MaxSparseIndexMapGap+lastSparseIndexUpdateOffset {
			sparseIndex.ReplaceOrInsert(sparseIndexItem{key: string(item.key), offset: oldNumDataBytes})
			lastSparseIndexUpdateOffset = oldNumDataBytes
		}
		if err = pickFirstErr(err1, err2, err3, err4); err != nil {
			return false
		}
		return true
	}
	memtable.data.Ascend(memTableHandle)
	if err != nil {
		return nil, NewInternalError(err)
	}
	// Persist the sparse index on disk
	numSparseIndexBytes := int64(0)
	sparseIndexHandle := func(item sparseIndexItem) bool {
		// Length of key
		binary.BigEndian.PutUint32(b4, uint32(len(item.key)))
		n1, err1 = f.Write(b4)
		// key bytes
		n2, err2 = f.Write([]byte(item.key))
		if len(item.key) > 100 {
			fmt.Println("wtf")
		}
		// offset in data section
		binary.BigEndian.PutUint64(b8, uint64(item.offset))
		n3, err3 = f.Write(b8)
		// Update num bytes written
		numSparseIndexBytes += int64(n1 + n2 + n3)
		if err = pickFirstErr(err1, err2, err3); err != nil {
			return false
		}
		return true
	}
	sparseIndex.Ascend(sparseIndexHandle)
	if err != nil {
		return nil, NewInternalError(err)
	}
	// Footer
	binary.BigEndian.PutUint64(b8, uint64(numDataBytes))
	if _, err = f.Write(b8); err != nil {
		return nil, NewInternalError(err)
	}
	binary.BigEndian.PutUint64(b8, uint64(numSparseIndexBytes))
	if _, err = f.Write(b8); err != nil {
		return nil, NewInternalError(err)
	}
	err = closeWriter(f, fRaw)
	fRaw = nil
	f = nil

	if err != nil {
		return nil, NewInternalError(err)
	}
	footer := &ssTableFooterInfo{
		numDataBytes:        numDataBytes,
		numSparseIndexBytes: numSparseIndexBytes,
	}
	ret := &SSTable{
		sparseIndex: sparseIndex,
		fileName:    filename,
		footer:      footer,
		readers:     createEmptyReaders(16),
		filter:      filter,
	}
	return ret, NoError
}

func (table *SSTable) nextSparseIndexItemFromFile(
	f *bufReaderWithSeek,
) (item sparseIndexItem, bytesRead int, dberr DbError) {
	var bytesReadCur int
	var err error
	bytesRead = 0
	b8 := make([]byte, 8)
	b4 := make([]byte, 4)
	if bytesReadCur, err = f.Read(b4); err != nil {
		return sparseIndexItem{}, bytesReadCur + bytesRead, NewInternalError(err)
	}
	bytesRead += bytesReadCur
	keyLen := binary.BigEndian.Uint32(b4)
	if keyLen > 100 {
		x, y := f.Seek(0, io.SeekCurrent)
		fmt.Println("what is this", x, y)
	}

	keyBytes := make([]byte, keyLen)
	if bytesReadCur, err = f.Read(keyBytes); err != nil {
		return sparseIndexItem{}, bytesReadCur + bytesRead, NewInternalError(err)
	}
	bytesRead += bytesReadCur
	item.key = string(keyBytes)

	if bytesReadCur, err = f.Read(b8); err != nil {
		return sparseIndexItem{}, bytesReadCur + bytesRead, NewInternalError(err)
	}
	if bytesReadCur != 8 {
		fmt.Println("wowow wowo")
	}
	bytesRead += bytesReadCur
	item.offset = int64(binary.BigEndian.Uint64(b8))
	return item, bytesRead, NoError
}

// nextSStableItemFromFile reads the next sstable item from disk. It returns the item value only if
// the key matches forKey. This will save some reads from disk.
func (table *SSTable) nextSStableItemFromFile(
	f *bufReaderWithSeek, forKey []byte, item *ssTableItemDisk,
) (nBytesRead int, dberr DbError) {
	nBytesReadCur := 0
	nBytesRead = 0
	b4 := make([]byte, 4)
	var err error
	if nBytesReadCur, err = io.ReadFull(f, b4); err != nil {
		return nBytesReadCur + nBytesRead, NewInternalError(err)
	}
	nBytesRead += nBytesReadCur
	item.setKeyLen(int(binary.BigEndian.Uint32(b4)))

	if nBytesReadCur, err = io.ReadFull(f, item.getKeySlice()); err != nil {
		return nBytesReadCur + nBytesRead, NewInternalError(err)
	}
	nBytesRead += nBytesReadCur

	if nBytesReadCur, err = io.ReadFull(f, b4); err != nil {
		return nBytesReadCur + nBytesRead, NewInternalError(err)
	}
	nBytesRead += nBytesReadCur
	valueLen := binary.BigEndian.Uint32(b4)

	if valueLen&uint32(1<<31) != 0 {
		item.isDeleted = true
	} else if bytes.Compare(item.key[:item.keyLen], forKey) == 0 {
		item.setValueLen(int(valueLen))
		if nBytesReadCur, err = io.ReadFull(f, item.getValueSlice()); err != nil {
			return nBytesReadCur + nBytesRead, NewInternalError(err)
		}
		nBytesRead += nBytesReadCur
	} else {
		nBytesRead += int(valueLen)
		if _, err = f.Discard(int(valueLen)); err != nil {
			return 0, NewInternalError(err)
		}
	}
	return nBytesRead, NoError
}

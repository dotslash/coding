package tree

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestInMemSStable(t *testing.T) {
	fuuid, _ := uuid.NewRandom()
	fName := fmt.Sprintf("/tmp/sstable_test_%s.sst", fuuid.String())
	t.Logf("ss table output: %s", fName)
	inMem := NewMemTable(defaultMemTableConfig)

	// empty set. key1 not exists.
	checkNotExists(t, "key1", inMem)

	// add key1 -> value1
	err := inMem.Put([]byte("key1"), []byte("value1"))
	assert.Equal(t, err.Success(), true)

	// Check key1 -> value1
	checkExists(t, "key1", "value1", inMem)

	// Delete key1
	err = inMem.Delete([]byte("key1"))
	assert.Equal(t, err.Success(), true)

	// key1 should not exist
	checkNotExists(t, "key1", inMem)

	// Put key1 -> value2
	err = inMem.Put([]byte("key1"), []byte("value2"))
	assert.Equal(t, err.Success(), true)

	// Verify key1 -> value2
	checkExists(t, "key1", "value2", inMem)

	// dump
	_, err = memtableToSSTable(fName, inMem)
	assert.Equal(t, err.Success(), true)
}

func TemplateTestLargeSStable(
	t *testing.T, config memTableConfig,
	maxTimeForReads time.Duration, readConcurrency int,
) TemplateTestLargeRes {
	fuuid, _ := uuid.NewRandom()
	fName := fmt.Sprintf("/tmp/sstable_test_%s.sst", fuuid.String())
	t.Logf("ss table output: %s", fName)
	memTable := NewMemTable(config)
	start := time.Now()

	ops, inmem, keys := generateOps(1000000, 0)
	t.Logf("NumOps: %v, num keys: %v", len(ops), len(keys))
	for i := 0; i < len(ops); i++ {
		op := ops[i]
		err := memTable.Put([]byte(op.key), []byte(op.value))
		assert.Equal(t, err.Success(), true)
		if i%100000 == 1 {
			t.Logf("Put %v entries in %v", i, time.Now().Sub(start))
		}
	}
	t.Logf("Put all entries in %v", time.Now().Sub(start))

	flushStart := time.Now()
	sstable, err := memtableToSSTable(fName, memTable)
	assert.Equal(t, err.Success(), true)
	decoded, rawErr := NewSsTableFromFile(fName)
	require.Nil(t, rawErr)
	flushDuration := time.Now().Sub(flushStart)
	t.Logf("ConvertToSegmentFile done in %v", flushDuration)

	readExistingEntries(t, 4, maxTimeForReads, 20000, keys, inmem, decoded)
	readNonExistingEntries(t, 4, maxTimeForReads, 20000, keys, decoded)

	readDuration, numReads := readExistingEntries(t, readConcurrency, maxTimeForReads, 1000*1000, keys, inmem, sstable)
	nonReadDuration, numNonReads := readNonExistingEntries(t, readConcurrency, maxTimeForReads, 1000*1000, keys, sstable)

	return TemplateTestLargeRes{
		putLatency:           flushStart.Sub(start),
		flushDuration:        flushDuration,
		readDuration:         readDuration,
		numReads:             numReads,
		nonReadDuration:      nonReadDuration,
		numNonReads:          numNonReads,
		sparseIndexDiskBytes: sstable.footer.numSparseIndexBytes,
		sparseIndexSize:      sstable.sparseIndex.Len(),
		readConcurrency:      readConcurrency,
	}

}

func TestLargeSstable1(t *testing.T) {
	config := memTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 32)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable2(t *testing.T) {
	config := memTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 16)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable3(t *testing.T) {
	config := memTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 8)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable4(t *testing.T) {
	config := memTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 4)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable5(t *testing.T) {
	config := memTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 2)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable6(t *testing.T) {
	config := memTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 1)
	t.Logf("config: %#v result: %#v", config, res)
}

package tree

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestInMemSStable(t *testing.T) {
	fuuid, _ := uuid.NewRandom()
	fName := fmt.Sprintf("/tmp/sstable_test_%s.dump", fuuid.String())
	t.Logf("ss table output: %s", fName)
	inMem := EmptyWithDefaultConfig(fName)

	// empty set. key1 not exists.
	checkNotExists(t, "key1", &inMem)

	// add key1 -> value1
	err := inMem.Put("key1", "value1")
	assert.Equal(t, err.Success(), true)

	// Check key1 -> value1
	checkExists(t, "key1", "value1", &inMem)

	// Delete key1
	err = inMem.Delete("key1")
	assert.Equal(t, err.Success(), true)

	// key1 should not exist
	checkNotExists(t, "key1", &inMem)

	// Put key1 -> value2
	err = inMem.Put("key1", "value2")
	assert.Equal(t, err.Success(), true)

	// Verify key1 -> value2
	checkExists(t, "key1", "value2", &inMem)

	// dump
	err = inMem.ConvertToSegmentFile()
	assert.Equal(t, err.Success(), true)
}

func TemplateTestLargeSStable(
	t *testing.T, config SSTableConfig,
	maxTimeForReads time.Duration, readConcurrency int,
) TemplateTestLargeRes {
	fuuid, _ := uuid.NewRandom()
	fName := fmt.Sprintf("/tmp/sstable_test_%s.dump", fuuid.String())
	t.Logf("ss table output: %s", fName)
	sstable := Empty(fName, config)
	start := time.Now()

	ops, inmem, keys := generateOps(1000*1000, 0.1)
	for i := 0; i < len(ops); i++ {
		op := ops[i]
		err := sstable.Put(op.key, op.value)
		assert.Equal(t, err.Success(), true)
		if i%100000 == 1 {
			t.Logf("Put %v entries in %v", i, time.Now().Sub(start))
		}
	}
	t.Logf("Put all entries in %v", time.Now().Sub(start))

	flushStart := time.Now()
	err := sstable.ConvertToSegmentFile()
	assert.Equal(t, err.Success(), true)
	flushDuration := time.Now().Sub(flushStart)
	t.Logf("ConvertToSegmentFile done in %v", flushDuration)

	readDuration, numReads := readExistingEntries(t, readConcurrency, maxTimeForReads, 1000*1000, keys, inmem, &sstable)
	nonReadDuration, numNonReads := readNonExistingEntries(t, readConcurrency, maxTimeForReads, 1000*1000, keys, &sstable)

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
	config := SSTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 32)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable2(t *testing.T) {
	config := SSTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 16)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable3(t *testing.T) {
	config := SSTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 8)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable4(t *testing.T) {
	config := SSTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 4)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable5(t *testing.T) {
	config := SSTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 2)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLargeSstable6(t *testing.T) {
	config := SSTableConfig{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLargeSStable(t, config, 10*time.Second, 1)
	t.Logf("config: %#v result: %#v", config, res)
}

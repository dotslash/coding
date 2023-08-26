package sstable

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func checkExists(t *testing.T, key, value string, sstable *SSTable) {
	actualValue, ok := sstable.Get(key)
	assert.Equal(t, ok, true)
	assert.Equal(t, actualValue, value)
}

func checkNotExists(t *testing.T, key string, sstable *SSTable) {
	_, ok := sstable.Get(key)
	assert.Equal(t, ok, false)
}

func TestInMem(t *testing.T) {
	fuuid, _ := uuid.NewRandom()
	fName := fmt.Sprintf("/tmp/sstable_test_%s.dump", fuuid.String())
	t.Logf("ss table output: %s", fName)
	inMem := EmptyWithDefaultConfig(fName)

	// empty set. key1 not exists.
	checkNotExists(t, "key1", &inMem)

	// add key1 -> value1
	err := inMem.Put("key1", "value1")
	assert.Equal(t, err, nil)

	// Check key1 -> value1
	checkExists(t, "key1", "value1", &inMem)

	// Delete key1
	err = inMem.Delete("key1")
	assert.Equal(t, err, nil)

	// key1 should not exist
	checkNotExists(t, "key1", &inMem)

	// Put key1 -> value2
	err = inMem.Put("key1", "value2")
	assert.Equal(t, err, nil)

	// Verify key1 -> value2
	checkExists(t, "key1", "value2", &inMem)

	// dump
	err = inMem.ConvertToSegmentFile()
	assert.Equal(t, err, nil)
}

type TemplateTestLargeRes struct {
	putLatency           time.Duration
	flushDuration        time.Duration
	readDuration         time.Duration
	sparseIndexSize      int
	sparseIndexDiskBytes int64
	numReads             int
}

func TemplateTestLarge(t *testing.T, config Config, maxTimeForReads time.Duration) TemplateTestLargeRes {
	fuuid, _ := uuid.NewRandom()
	fName := fmt.Sprintf("/tmp/sstable_test_%s.dump", fuuid.String())
	t.Logf("ss table output: %s", fName)
	inMem := Empty(fName, config)
	start := time.Now()
	for i := 0; i < 1000*1000; i++ {
		key := fmt.Sprintf("key%v", i)
		value := fmt.Sprintf("value%v", i)
		err := inMem.Put(key, value)
		assert.Equal(t, err, nil)
		if i%100000 == 0 {
			t.Logf("Put %v entries in %v", i, time.Now().Sub(start))
		}
	}
	t.Logf("Put all entries in %v", time.Now().Sub(start))
	flushStart := time.Now()
	err := inMem.ConvertToSegmentFile()
	assert.Equal(t, err, nil)
	t.Logf("ConvertToSegmentFile done in %v", time.Now().Sub(flushStart))

	readStart := time.Now()
	numReads := 0
	for ; numReads < 1000*1000 && time.Now().Sub(readStart) < maxTimeForReads; numReads++ {
		key := fmt.Sprintf("key%v", numReads)
		value := fmt.Sprintf("value%v", numReads)
		checkExists(t, key, value, &inMem)
		if numReads%1000 == 1 {
			t.Logf("Read %v entries in %v", numReads, time.Now().Sub(readStart))
		}
	}
	t.Logf("Read %v entries in %v", numReads, time.Now().Sub(readStart))
	return TemplateTestLargeRes{
		putLatency:           flushStart.Sub(start),
		flushDuration:        readStart.Sub(flushStart),
		readDuration:         time.Now().Sub(readStart),
		sparseIndexDiskBytes: inMem.footer.numSparseIndexBytes,
		sparseIndexSize:      inMem.sparseIndex.Len(),
		numReads:             numReads,
	}

}

func TestLarge1(t *testing.T) {
	config := Config{DefaultMaxMemTableSize, 16 * 1024}
	res := TemplateTestLarge(t, config, 10*time.Second)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLarge2(t *testing.T) {
	config := Config{DefaultMaxMemTableSize, 32 * 1024}
	res := TemplateTestLarge(t, config, 10*time.Second)
	t.Logf("config: %#v result: %#v", config, res)
}
func TestLarge3(t *testing.T) {
	config := Config{DefaultMaxMemTableSize, 64 * 1024}
	res := TemplateTestLarge(t, config, 10*time.Second)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLarge4(t *testing.T) {
	config := Config{DefaultMaxMemTableSize, 128 * 1024}
	res := TemplateTestLarge(t, config, 10*time.Second)
	t.Logf("config: %#v result: %#v", config, res)
}

func TestLarge5(t *testing.T) {
	config := Config{DefaultMaxMemTableSize, 256 * 1024}
	res := TemplateTestLarge(t, config, 10*time.Second)
	t.Logf("config: %#v result: %#v", config, res)
}

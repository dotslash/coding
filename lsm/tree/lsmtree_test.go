package tree

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestInMem(t *testing.T) {
	fuuid, _ := uuid.NewRandom()
	fName := fmt.Sprintf("/tmp/lsmtree_test_%s.dump", fuuid.String())
	assert.NoError(t, os.Mkdir(fName, 0755))

	t.Logf("ss table output: %s", fName)
	inMem := LSMTree{dataLocation: fName}

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

}

func TemplateTestLarge(
	t *testing.T,
	maxTimeForReads time.Duration, readConcurrency int,
) TemplateTestLargeRes {
	fuuid, _ := uuid.NewRandom()
	fName := fmt.Sprintf("/tmp/lsmtree_test_%s.dump", fuuid.String())
	assert.NoError(t, os.Mkdir(fName, 0755))
	t.Logf("ss table output: %s", fName)

	sstableSize := 1000 * 1000
	numSStables := 10
	ops, inmem, keys := generateOps(sstableSize*numSStables, 0.5)
	t.Logf("Generated %v ops, inmem size = %v numkeys size = %v", len(ops), len(inmem), len(keys))
	start := time.Now()
	table := LSMTree{dataLocation: fName}
	for i := 0; i < numSStables*sstableSize; i++ {
		op := ops[i]
		err := table.Put(op.key, op.value)
		assert.Equal(t, err, NoError, err.Error())

		if i > sstableSize && i%sstableSize == 0 {
			t.Logf("Put %v entries in %v", i, time.Now().Sub(start))
			flushStart := time.Now()
			err := table.flushToDisk()
			assert.Equal(t, err.Success(), true, err.Error())
			t.Logf("ConvertToSegmentFile done in %v", time.Now().Sub(flushStart))
		}
	}
	t.Logf("Put all entries in %v", time.Now().Sub(start))

	readDuration, numReads := readExistingEntries(t, readConcurrency, maxTimeForReads, 1000*1000, keys, inmem, &table)
	nonReadDuration, numNonReads := readNonExistingEntries(t, readConcurrency, maxTimeForReads, 1000*1000, keys, &table)

	t.Logf("Read %v entries in %v", numReads, readDuration)
	t.Logf("Read %v non existing entries in %v", numNonReads, nonReadDuration)

	return TemplateTestLargeRes{
		readDuration:    readDuration,
		numReads:        numReads,
		nonReadDuration: nonReadDuration,
		numNonReads:     numNonReads,
		readConcurrency: readConcurrency,
	}

}

func TestLarge1(t *testing.T) {
	res := TemplateTestLarge(t, 10*time.Second, 32)
	t.Logf("result: %#v", res)
}

func TestLarge2(t *testing.T) {
	res := TemplateTestLarge(t, 10*time.Second, 8)
	t.Logf("result: %#v", res)
}

func TestLarge3(t *testing.T) {
	res := TemplateTestLarge(t, 10*time.Second, 1)
	t.Logf("result: %#v", res)
}

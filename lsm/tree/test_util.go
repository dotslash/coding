package tree

import (
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
	"math/rand"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func checkExists(t *testing.T, key, value string, kv KVStore) {
	actualValue, err := kv.Get(stringToBytesUnsafe(key))
	assert.NoError(t, err.GetError(), "for %s->%s", key, value)
	assert.Equal(t, 0, bytes.Compare(stringToBytesUnsafe(value), actualValue), "for %s->%s", key, value)
}

func stringToBytesUnsafe(s string) []byte {
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: stringHeader.Data,
		Len:  stringHeader.Len,
		Cap:  stringHeader.Len,
	}))
}
func checkNotExists(t *testing.T, key string, kv KVStore) {
	_, err := kv.Get(stringToBytesUnsafe(key))
	assert.Equal(t,
		true,
		err.ErrorType == KeyNotExists || err.ErrorType == KeyMarkedAsDeleted,
		"for %s", key,
	)
}

type putOp struct {
	key, value string
}

type TemplateTestLargeRes struct {
	putLatency           time.Duration
	flushDuration        time.Duration
	readDuration         time.Duration
	sparseIndexSize      int
	sparseIndexDiskBytes int64
	numReads             int
	readConcurrency      int
	nonReadDuration      time.Duration
	numNonReads          int
}

func generateOps(numOps int, overrideProbability float32) (ops []putOp, finalKV map[string]string, keys []string) {
	finalKV = make(map[string]string)
	ops = make([]putOp, 0)
	for i := 0; i < numOps; i++ {
		keyIndex := i
		if i > 0 && rand.Float32() < overrideProbability {
			keyIndex = rand.Intn(i)
		}
		key := fmt.Sprintf("Key_%v", keyIndex)
		value := fmt.Sprintf("Value_%v", i)
		ops = append(ops, putOp{key, value})
		finalKV[key] = value
	}
	keys = make([]string, 0)
	for k := range finalKV {
		keys = append(keys, k)
	}
	return ops, finalKV, keys
}

func readNonExistingEntries(
	t *testing.T, readConcurrency int,
	maxTimeForReads time.Duration, maxOps int,
	keys []string, store KVStore,
) (nonReadDuration time.Duration, nops int) {
	t.Logf(
		"readNonExistingEntries: Will read upto %v entries with a concurrency of %v. Timeout:%v",
		maxOps, readConcurrency, maxTimeForReads,
	)

	sem := semaphore.NewWeighted(int64(readConcurrency))
	numReads := 0
	readStart := time.Now()
	batchSize := 1000
	for ; numReads < maxOps && time.Now().Sub(readStart) < maxTimeForReads; numReads += batchSize {
		assert.Equal(t, sem.Acquire(context.TODO(), 1), nil)
		go func(startIndex int) {
			defer sem.Release(1)
			for i := 0; i < batchSize; i++ {
				keyToTest := keys[rand.Intn(len(keys))] + "zzz_not_exists"
				// Actual test
				checkNotExists(t, keyToTest, store)
				curInd := startIndex + i
				if curInd%100000 == 1 {
					t.Logf("readNonExistingEntries %v entries in %v", curInd, time.Now().Sub(readStart))
				}
			}
		}(numReads)
	}
	assert.Equal(t, sem.Acquire(context.TODO(), int64(readConcurrency)), nil)
	t.Logf("readNonExistingEntries %v entries in %v", numReads, time.Now().Sub(readStart))
	return time.Now().Sub(readStart), numReads
}

func readExistingEntries(
	t *testing.T, readConcurrency int,
	maxTimeForReads time.Duration, maxOps int,
	keys []string, inmem map[string]string, store KVStore,
) (readDuration time.Duration, nops int) {
	t.Logf(
		"readExistingEntries: Will read upto %v entries with a concurrency of %v. Timeout:%v",
		maxOps, readConcurrency, maxTimeForReads,
	)
	sem := semaphore.NewWeighted(int64(readConcurrency))
	numReads := 0

	batchSize := 1000
	readStart := time.Now()
	for ; numReads < maxOps && time.Now().Sub(readStart) < maxTimeForReads; numReads += batchSize {
		assert.Equal(t, sem.Acquire(context.TODO(), 1), nil)
		go func(_startIndex int) {
			defer sem.Release(1)
			for curInd := _startIndex; curInd < _startIndex+batchSize; curInd++ {
				keyToTest := keys[rand.Intn(len(keys))]
				expectedValue := inmem[keyToTest]
				// Actual test
				checkExists(t, keyToTest, expectedValue, store)
				if curInd%100000 == 1 {
					t.Logf("readExistingEntries %v entries in %v", curInd, time.Now().Sub(readStart))
				}
			}
		}(numReads)
	}
	assert.Equal(t, sem.Acquire(context.TODO(), int64(readConcurrency)), nil)
	t.Logf("readExistingEntries %v entries in %v", numReads, time.Now().Sub(readStart))
	return time.Now().Sub(readStart), numReads
}

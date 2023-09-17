package tree

import (
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
	"math/rand"
	"testing"
	"time"
)

func checkExists(t *testing.T, key, value string, kv KVStore) {
	actualValue, err := kv.Get([]byte(key))
	assert.NoError(t, err.GetError(), "for %s->%s", key, value)
	assert.Equal(t, 0, bytes.Compare([]byte(value), actualValue), "for %s->%s", key, value)
}

func checkNotExists(t *testing.T, key string, kv KVStore) {
	_, err := kv.Get([]byte(key))
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
		key := fmt.Sprintf("value%v", keyIndex)
		value := fmt.Sprintf("value%v", i)
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
	for ; numReads < maxOps && time.Now().Sub(readStart) < maxTimeForReads; numReads++ {
		assert.Equal(t, sem.Acquire(context.TODO(), 1), nil)
		go func(numReadsArg int) {
			defer sem.Release(1)
			keyToTest := keys[rand.Intn(len(keys))] + "zzz_not_exists"
			// Actual test
			checkNotExists(t, keyToTest, store)
			if numReadsArg%100000 == 1 {
				t.Logf("readNonExistingEntries %v entries in %v", numReadsArg, time.Now().Sub(readStart))
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
	readStart := time.Now()
	for ; numReads < maxOps && time.Now().Sub(readStart) < maxTimeForReads; numReads++ {
		assert.Equal(t, sem.Acquire(context.TODO(), 1), nil)
		go func(numReadsArg int) {
			defer sem.Release(1)
			keyToTest := keys[rand.Intn(len(keys))]
			expectedValue := inmem[keyToTest]
			// Actual test
			checkExists(t, keyToTest, expectedValue, store)
			if numReadsArg%100000 == 1 {
				t.Logf("readExistingEntries %v entries in %v", numReadsArg, time.Now().Sub(readStart))
			}
		}(numReads)
	}
	assert.Equal(t, sem.Acquire(context.TODO(), int64(readConcurrency)), nil)
	t.Logf("readExistingEntries %v entries in %v", numReads, time.Now().Sub(readStart))
	return time.Now().Sub(readStart), numReads
}

package filter

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
	"sync/atomic"
	"testing"
	"time"
)

const numReadRoundsToTest = 100
const numKeysToTest = 100 * 1000
const fpRateToTest = 0.01
const readParallelismToTest = 10

func TemplateTestFilter(
	t *testing.T, numKeys int, numReadRounds int, cf Filter,
	emptyFilter Filter,
) {
	insertedKeys := make([][]byte, 0)
	nonInsertedKeys := make([][]byte, 0)
	for i := 0; i < numKeys; i++ {
		insertedKeys = append(insertedKeys, []byte(fmt.Sprintf("TemplateTestCuckoo_%v", i)))
		nonInsertedKeys = append(nonInsertedKeys, []byte(fmt.Sprintf("TemplateTestCuckoo_%v_zzz_not_exists", i)))
	}
	start := time.Now()
	for _, key := range insertedKeys {
		cf.Insert(key)
	}
	t.Logf("Took %v to insert %v entries", time.Now().Sub(start), numKeys)

	// Freeze the filter if possible
	if freezeable, ok := cf.(Freezable); ok {
		freezeable.Freeze()
	}

	// Encode + decode
	encoded, err := cf.Encode()
	assert.Nil(t, err)
	// Decode the encoded  filter and assert that all keys exist.
	decodedCf, err := emptyFilter.Decode(encoded)
	assert.Nil(t, err)
	assert.Equal(t, emptyFilter, decodedCf)

	checkEquivalent(t, cf, decodedCf, insertedKeys, nonInsertedKeys)

	numPassingInserts, numPassingNonInserts :=
		testReads(t, numReadRounds, cf, insertedKeys, nonInsertedKeys, readParallelismToTest)
	testReads(t, 1, cf, insertedKeys, nonInsertedKeys, 1)

	t.Logf(
		"numPassingNonInserts:%v%% numPassingInserts:%v%% num keys:%v Datasctucture Size:%v",
		float32(numPassingNonInserts*100)/float32(numReadRounds*numKeys), float32(numPassingInserts*100)/float32(numReadRounds*numKeys), len(insertedKeys), len(encoded),
	)

}

func checkEquivalent(t *testing.T, cf1 Filter, cf2 Filter, keySets ...[][]byte) {
	for _, keys := range keySets {
		for _, k := range keys {
			assert.Equal(t, cf1.Lookup(k), cf2.Lookup(k), "Failed at %v", string(k))
		}
	}

}

func testReads(t *testing.T, numReadRounds int, cf Filter, insertedKeys [][]byte, nonInsertedKeys [][]byte, parallelism int64) (int32, int32) {
	numPassingInserts := int32(0)
	numPassingNonInserts := int32(0)
	readStart := time.Now()
	sem := semaphore.NewWeighted(parallelism)
	for iter := 0; iter < numReadRounds; iter++ {
		sem.Acquire(context.TODO(), 1)
		localNumPassingInserts := int32(0)
		localNumPassingNonInserts := int32(0)
		go func() {
			defer sem.Release(1)
			for i := 0; i < len(insertedKeys); i++ {
				if cf.Lookup(insertedKeys[i]) {
					localNumPassingInserts++
				}
				if cf.Lookup(nonInsertedKeys[i]) {
					localNumPassingNonInserts++
				}
			}
			atomic.AddInt32(&numPassingInserts, localNumPassingInserts)
			atomic.AddInt32(&numPassingNonInserts, localNumPassingNonInserts)
		}()
	}
	sem.Acquire(context.TODO(), parallelism)
	t.Logf(
		"Took %v to read %v entries with a parallelism of %v",
		time.Now().Sub(readStart), 2*numReadRounds*len(insertedKeys), parallelism,
	)
	require.Equal(t, numPassingInserts, int32(numReadRounds*len(insertedKeys)))
	return numPassingInserts, numPassingNonInserts
}

func TestExactSizeCuckoo(t *testing.T) {
	cf := (&CuckooFilter{}).Init(numKeysToTest, fpRateToTest)
	TemplateTestFilter(t, numKeysToTest, numReadRoundsToTest, cf, &CuckooFilter{})
}

func TestScalableCuckoo(t *testing.T) {
	cf := (&ScalableCuckooFilter{}).Init(numKeysToTest, fpRateToTest)
	TemplateTestFilter(t, numKeysToTest, numReadRoundsToTest, cf, &ScalableCuckooFilter{})
}

func TestBitsAndBloomsFilter(t *testing.T) {
	f := (&BitsAndBloomsFilter{}).Init(numKeysToTest, fpRateToTest)
	TemplateTestFilter(t, numKeysToTest, numReadRoundsToTest, f, &BitsAndBloomsFilter{})
}

func TestBoomCuckooFilter(t *testing.T) {
	f := (&BoomCuckooFilter{}).Init(numKeysToTest, fpRateToTest)
	TemplateTestFilter(t, numKeysToTest, numReadRoundsToTest, f, &BoomCuckooFilter{})
}

func TestBoomBloom(t *testing.T) {
	f := (&BoomBloomFilter{}).Init(numKeysToTest, fpRateToTest)
	TemplateTestFilter(t, numKeysToTest, numReadRoundsToTest, f, &BoomBloomFilter{})
}

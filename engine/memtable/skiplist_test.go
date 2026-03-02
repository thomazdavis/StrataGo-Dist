package memtable

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipList_BasicOps(t *testing.T) {
	list := NewSkipList()
	assert.NotNil(t, list)

	list.Put([]byte("user:101"), []byte("Barca"))
	list.Put([]byte("user:102"), []byte("Ajax"))

	assert.Equal(t, 2, list.Size)

	val, found := list.Get([]byte("user:101"))
	assert.True(t, found)
	assert.Equal(t, []byte("Barca"), val)

	val, found = list.Get([]byte("user:102"))
	assert.True(t, found)
	assert.Equal(t, []byte("Ajax"), val)

	_, found = list.Get([]byte("user:999"))
	assert.False(t, found)
}

func TestSkipList_Update(t *testing.T) {
	list := NewSkipList()

	list.Put([]byte("config"), []byte("v1"))

	val, _ := list.Get([]byte("config"))
	assert.Equal(t, []byte("v1"), val)

	list.Put([]byte("config"), []byte("v2"))

	val, _ = list.Get([]byte("config"))
	assert.Equal(t, []byte("v2"), val)

	assert.Equal(t, 1, list.Size)
}

func TestSkipList_Concurrency(t *testing.T) {
	list := NewSkipList()

	var wg sync.WaitGroup
	numRoutines := 1000

	// 1000 concurrent writes
	for i := range numRoutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", i))
			val := []byte(fmt.Sprintf("val-%d", i))
			list.Put(key, val)
		}(i)
	}

	wg.Wait()

	// If the locks work, Size should be exactly 1000.
	// If they failed, we might have lost data due to race conditions.
	assert.Equal(t, numRoutines, list.Size)
}

package sstable

import (
	"bytes"
	"container/heap"
)

type mergeItem struct {
	key     []byte
	val     []byte
	iterIdx int
	iter    *Iterator
}

type mergeHeap []*mergeItem

func (h mergeHeap) Len() int {
	return len(h)
}

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp == 0 {
		// If keys are identical, we pop the newer file first
		// We pass iterators ordered from new to old
		return h[i].iterIdx < h[j].iterIdx
	}
	return cmp < 0
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(*mergeItem))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// k-way merge algorithm
// Merge takes a list of Iterators (ordered from newest to oldest)
// and writes their deduplicated, sorted contents to the builder.
func Merge(iters []*Iterator, builder *Builder) error {
	h := &mergeHeap{}
	heap.Init(h)

	// Seed the heap with the first item from each file
	for i, it := range iters {
		if it.Next() {
			heap.Push(h, &mergeItem{
				key:     append([]byte{}, it.Key()...), // Copy to avoid memory mutation
				val:     append([]byte{}, it.Value()...),
				iterIdx: i,
				iter:    it,
			})
		} else if err := it.Error(); err != nil {
			return err
		}
	}

	var lastKey []byte

	// Process the heap until all files are completely read
	for h.Len() > 0 {
		item := heap.Pop(h).(*mergeItem)

		// Deduplication Logic
		if lastKey == nil || !bytes.Equal(lastKey, item.key) {

			// Write to the new SSTable
			if err := builder.Add(item.key, item.val); err != nil {
				return err
			}

			// Remember this key so we can skip older versions of it
			// which might come in later iteratiosn
			lastKey = append([]byte{}, item.key...)
		}

		// Advance the iterator that this item came from
		if item.iter.Next() {
			item.key = append([]byte{}, item.iter.Key()...)
			item.val = append([]byte{}, item.iter.Value()...)
			heap.Push(h, item)
		} else if err := item.iter.Error(); err != nil {
			return err
		}
	}

	// Finalize the new merged file
	return builder.Finish()
}

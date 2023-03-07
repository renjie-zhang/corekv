package utils

import (
	"bytes"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"sync"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	//implement me here!!!
	header := &Element{
		levels: make([]*Element, defaultMaxLevel),
		entry:  nil,
		score:  0,
	}

	return &SkipList{
		header:   header,
		rand:     r,
		maxLevel: defaultMaxLevel - 1,
		length:   0,
		lock:     sync.RWMutex{},
		size:     0,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()
	//implement me here!!!

	pres := make([]*Element, list.maxLevel+1)
	key := data.Key
	keyScore := list.calcScore(data.Key)
	header := list.header
	maxLevel := list.maxLevel
	pre := header
	for i := maxLevel; i >= 0; i-- {
		for next := pre.levels[i]; next != nil; next = pre.levels[i] {
			if compare := list.compare(keyScore, key, next); compare <= 0 {
				if compare == 0 {
					// update data
					next.entry = data
					return nil
				} else {
					pre = next
				}
			} else {
				break
			}
		}
		pres[i] = pre
	}

	randLevel, keyScore := list.randLevel(), list.calcScore(key)
	e := newElement(keyScore, data, randLevel)
	for i := randLevel; i >= 0; i-- {
		next := pres[i].levels[i]
		pres[i].levels[i] = e
		e.levels[i] = next
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//implement me here!!!
	list.lock.RLock()
	defer list.lock.RUnlock()
	score := list.calcScore(key)
	maxLevel := list.maxLevel
	pre := list.header
	for i := maxLevel; i >= 0; i-- {
		for next := pre.levels[i]; next != nil; next = pre.levels[i] {
			if compare := list.compare(score, key, next); compare <= 0 {
				if compare == 0 {
					return next.entry
				} else {
					pre = next
				}
			} else {
				break
			}
		}
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	//implement me here!!!
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	//implement me here!!!
	for i := 0; i < list.maxLevel; i++ {
		if list.rand.Intn(2) == 0 {
			return i
		}
	}
	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return list.size
}

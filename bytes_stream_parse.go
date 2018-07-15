package main

import (
	"bytes"
	"fmt"
	"sync"
)

var str []byte = []byte("nopnopnop:magic:word word word:magic:another wo:magic:rds here:magic:nopnopnop")
var magic = ":magic:"
var mlen = len(magic)

func main() {
	fifo := New()
	fifo.SetLen(mlen)

	slice := [][]byte{}
	word := []byte{}

	m := false
	for _, s := range str {
		_ = magic
		fifo.Add(s)

		word = append(word, s)

		if fifo.Compare([]byte(magic)) {
			if m {
				slice = append(slice, word[:len(word)-mlen])
			}
			word = []byte(nil)
			m = true
		}
	}

	for k, v := range slice {
		fmt.Printf("%d -> %s\n", k, v)
	}
}

func New() *T {
	t := new(T)
	t.len = 3
	return t
}

type T struct {
	m   []byte
	len int
	mu  sync.Mutex
}

func (t *T) SetLen(l int) {
	t.mu.Lock()
	t.len = l
	t.mu.Unlock()
}

func (t *T) Add(s byte) {
	t.mu.Lock()
	t.m = append(t.m, s)
	if len(t.m) > t.len {
		t.m = t.m[len(t.m)-t.len:]
	}
	t.mu.Unlock()
}

func (t *T) Value() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.m
}

func (t *T) Compare(b []byte) bool {
	return bytes.Compare(t.Value(), b) == 0
}

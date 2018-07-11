package main

import (
	"fmt"
	"runtime"
	"time"
)

func main() {
	e := NewT(1*time.Second, func(timeout bool) {
		fmt.Println("timeout or event", timeout)
	})
	_ = e
	// e.Event()

	go func() {
		for {
		}
	}()
	runtime.Goexit()
}

func NewT(d time.Duration, fn func(bool)) *T {
	t := new(T)
	t.d = d
	t.F = fn
	t.timer = time.NewTimer(d)
	t.event = make(chan struct{})
	go t.worker()
	return t
}

type T struct {
	d     time.Duration
	timer *time.Timer
	event chan struct{}
	F     func(bool)
}

func (t *T) worker() {
	select {
	case <-t.timer.C:
		t.run(true)
	case <-t.event:
		t.run(false)
	}
}

func (t *T) run(to bool) {
	t.timer.Stop()
	t.F(to)
}

func (t *T) Reset() {
	t.timer.Reset(t.d)
}

func (t *T) Event() {
	close(t.event)
}

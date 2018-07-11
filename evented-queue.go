package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

func main() {

	unsub := lt.Subscribe(func(s string) {
		fmt.Println("got message", s)
	})

	go func() {
		time.Sleep(10 * time.Second)
		unsub()
		lt.Subscribe(func(s string) {
			fmt.Println("[resub] got message", s)
		})
	}()

	lt.PCap = 5
	// call on lt.LCap limit
	lt.PC = func(c int) {
		fmt.Println("Primary queue limit reached: ", c)
	}

	lt.SCap = 3
	// call on lt.GCap limit
	lt.SC = func(c int) {
		fmt.Println("Secondary queue limit reached: ", c)
	}

	// mq.subscribe(fn(msg) { lt.WriteSecondary(msg) })
	go func() {
		for {
			time.Sleep(1 * time.Second)
			lt.WriteSecondary(fmt.Sprintf("global-%d", time.Now().Unix()))
		}
	}()

	// read from local file
	go func() {
		for i := 0; i <= 10; i++ {
			time.Sleep(500 * time.Millisecond)
			lt.WritePrimary(fmt.Sprintf("local-%d", i))

		}
		// local file parse done, switch to secondary queue
		lt.Switch()
	}()

	// run forever
	runtime.Goexit()
}

var lt = NewT()

func NewT() *T {
	t := new(T)
	t.l = &t.l1
	t.mu = &sync.Mutex{}
	t.cond = sync.NewCond(t.mu)

	t.PCap = 5
	t.SCap = 3
	return t
}

type T struct {
	l  *L
	l1 L
	l2 L

	sw   bool
	scnt int

	mu   *sync.Mutex
	cond *sync.Cond
	PC   func(int)
	SC   func(int)
	PCap int
	SCap int
}

func (t *T) WritePrimary(m string) {
	if t.scnt == 0 {
		return
	}
	t.mu.Lock()
	t.l1.Write1(m)
	if t.l1.Len() > t.PCap {
		if t.PC != nil {
			t.PC(t.l1.Len())
		}
	}
	t.cond.Signal()
	t.mu.Unlock()
}

func (t *T) WriteSecondary(m string) {
	if t.scnt == 0 {
		return
	}
	t.mu.Lock()
	t.l2.Write1(m)
	if t.l2.Len() > t.SCap {
		if t.SC != nil {
			t.SC(t.l2.Len())
		}
	}
	if t.sw == true && t.l == &t.l2 {
		t.cond.Signal()
	}
	t.mu.Unlock()
}

func (t *T) Switch() {
	t.mu.Lock()
	t.sw = true
	t.mu.Unlock()
}

func (t *T) process() (string, error) {
	m, e := t.l.process()
	if t.sw && e != nil {
		t.l = &t.l2
		return t.l.process()
	}
	return m, e
}

func (t *T) process2() string {
	lt.mu.Lock()
	m, e := lt.process()
	if e != nil {
		lt.cond.Wait()
		m, e = lt.process()
	}
	lt.mu.Unlock()
	return m
}

func (t *T) Subscribe(fn func(string)) (ubsub func()) {
	t.scnt++

	ch := make(chan string)
	ctx, cf := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case m, ok := <-ch:
				if ok {
					fn(m)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case ch <- t.process2():
			case <-ctx.Done():
				close(ch)
				t.scnt--
				return
			}
		}
	}()
	return func() { cf() }
}

// linked list node
type LI struct {
	Value string
	next  *LI
}

// linked list
type L struct {
	head *LI
	tail *LI
	cnt  int
}

func (l *L) process() (string, error) {
	if l.head == nil {
		return "", fmt.Errorf("error")
	}
	msg := l.head
	if msg != nil {
		l.cnt--
		l.head = msg.next
		if l.head == nil {
			l.tail = nil
		}
	}
	return msg.Value, nil
}

func (l *L) Write1(v string) {
	li := &LI{v, nil}
	if l.head == nil {
		l.head = li
		l.tail = li
	} else {
		l.tail.next = li
		l.tail = li
	}
	l.cnt++
}

func (l *L) Len() int {
	return l.cnt
}

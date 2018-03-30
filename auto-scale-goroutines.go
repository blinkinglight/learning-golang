package main

import (
	"context"
	"log"
	"sync"
	"time"
)

type S struct {
	run     bool
	ctx     context.Context
	cf      context.CancelFunc
	Payload struct{}
}

type T struct {
	m  map[string]*S
	mu sync.RWMutex
}

func (mb T) Get(name string) bool {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.m[name].run
}

func (mb *T) Set(name string, b bool) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.m[name].run = b
}

func (mb *T) Create(name string) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	mb.m[name] = &S{true, ctx, cancel, struct{}{}}
}

func (mb T) GetContext(name string) context.Context {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.m[name].ctx
}

func (mb T) Stop(name string) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.m[name].cf()
	mb.m[name].run = false
}

var mb T

var ch chan string

func main() {
	mb.m = make(map[string]*S)
	ch = make(chan string)

	Start("first")

	go func() {
		for {
			select {
			case <-time.After(1 * time.Second):
				ch <- "string? ;)"
			}
		}
	}()
	go func() {
		for {
			time.Sleep(1 * time.Second)
		}
	}()
	time.Sleep(3 * time.Second)
	Start("second")
	time.Sleep(3 * time.Second)
	Stop("first")
	Stop("second")

	m := make(chan struct{})
	<-m
}

func Stop(name string) {
	mb.Stop(name)
}

func Start(name string) {
	mb.Create(name)

	go func() {
		defer func() {
			log.Println("quiting...")
		}()
		for mb.Get(name) {
			select {
			case <-mb.GetContext(name).Done():
				log.Printf("context done")
			case <-time.After(1 * time.Second):
				log.Println("do job.", name)
			case m := <-ch:
				log.Printf("%v -> message from ch: %v", name, m)
			}
		}
	}()
}

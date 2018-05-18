package main

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func main() {

	t := New()

	// for testing only
	// use lru or other cache as MQ topic(file/url)
	ht := http.Client{}
	r, _ := ht.Get("http://mirror.vpsnet.com/centos/7/isos/x86_64/CentOS-7-x86_64-DVD-1708.iso")
	go func() {
		io.Copy(t, r.Body)
		r.Body.Close()
		t.Done()
	}()

	http.HandleFunc("/out.iso", func(w http.ResponseWriter, r *http.Request) {
		//TODO: copy headers
		t.WriteTo(w)
	})

	http.ListenAndServe(":9002", nil)

	runtime.Goexit()
}

func New() *T {
	t := new(T)
	t.d = [][]byte{}
	t.dc = make(chan struct{})
	t.mu = &sync.RWMutex{}
	return t
}

type T struct {
	d   [][]byte
	cnt int32
	dc  chan struct{}
	eof bool
	mu  *sync.RWMutex
}

func (t *T) Done() {
	t.eof = true
	close(t.dc)
}

func (t *T) WriteTo(w io.Writer) {
	ch, cfn := t.Read()
	defer func() {
		cfn()
	}()
	for b := range ch {
		_, e := w.Write(b)
		if e != nil {
			return
		}
	}
}

func (t *T) Read() (chan []byte, context.CancelFunc) {
	ch := make(chan []byte)
	var num int32 = 0
	fln := func() int {
		t.mu.RLock()
		defer t.mu.RUnlock()
		return len(t.d)
	}
	for fln() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cfn := context.WithCancel(context.TODO())
	fn := func() []byte {
		t.mu.RLock()
		defer t.mu.RUnlock()
		return t.d[num]
	}
	go func() {
		defer func() {
			close(ch)
		}()
		for {

			pos := atomic.LoadInt32(&t.cnt)
			for pos > num {
				select {
				case ch <- fn():
				case <-ctx.Done():
					return
				}
				num++
			}
			if t.eof == true && atomic.LoadInt32(&t.cnt) == num {
				return
			}
			if t.eof == false {
				<-t.dc
			}
		}

	}()
	return ch, cfn
}

func (t *T) Write(chunk []byte) (n int, err error) {
	t.mu.Lock()
	t.d = append(t.d, chunk)
	t.mu.Unlock()
	atomic.AddInt32(&t.cnt, 1)

	close(t.dc)
	t.dc = make(chan struct{})

	return len(chunk), nil
}

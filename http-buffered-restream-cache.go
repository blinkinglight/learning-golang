package main

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sync"
	"time"
)

type Cache struct {
	Cache []byte
	Pos   int
	Eof   bool
	lock  sync.RWMutex
}

func (c *Cache) Read(r io.Reader) {
	pos := 0
	rl := 2 << 16
	data := []byte{}
	for {
		if c.Pos <= pos && !c.Eof {
			continue
		}
		if pos+rl < c.Pos {
			c.lock.RLock()
			data = c.Cache[pos : rl+pos]
			c.lock.RUnlock()
			pos = pos + rl
			r.Read(data)
		} else {
			c.lock.RLock()
			data = c.Cache[pos:len(c.Cache)]
			c.lock.RUnlock()
			pos = pos + (len(c.Cache) - pos)
			r.Read(data)
			if c.Eof {
				return
			}
		}

	}
}

func (c *Cache) Write(b []byte) (int, error) {
	c.lock.Lock()
	c.Cache = append(c.Cache, b...)
	c.lock.Unlock()
	c.Pos = len(c.Cache) - 1

	return len(b), nil
}

type Readeris struct {
	num int
	out io.Writer
}

func (r *Readeris) Read(b []byte) (int, error) {
	r.out.Write(b)
	return len(b), nil
}

type Writeris struct {
	num int
}

func (w *Writeris) Write(b []byte) (int, error) {
	fmt.Printf("%d %s\n", w.num, b)
	return len(b), nil
}

var c *Cache
var urls = make(map[string]*Cache)
var lock sync.RWMutex

func ht(w http.ResponseWriter, r *http.Request) {
	readeris := new(Readeris)
	readeris.num = 30
	readeris.out = w
	c.Read(readeris)
}

func rd(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	var ok bool
	var cc *Cache
	lock.Lock()
	cc, ok = urls[r.URL.Path]
	// lock.Unlock()
	if !ok {
		// lock.Lock()
		cc = new(Cache)
		cc.Cache = []byte{}
		urls[path] = cc
		lock.Unlock()
		go func() {
			url := "http://127.0.0.1" + path
			fmt.Println("downloading: " + url)
			rsp, _ := (&http.Client{}).Get(url)
			defer rsp.Body.Close()
			_, e := io.Copy(cc, rsp.Body)
			if e != nil {
				fmt.Printf("error: %v\n", e)
			}
			cc.Eof = true
		}()
	} else [
		lock.Unlock()
	]
	readeris := new(Readeris)
	readeris.num = 30
	readeris.out = w
	cc.Read(readeris)
}

func main() {

	c = new(Cache)
	c.Cache = []byte{}

	http.HandleFunc("/", rd)
	fmt.Println(http.ListenAndServe(":8080", nil))

	runtime.Goexit()
}

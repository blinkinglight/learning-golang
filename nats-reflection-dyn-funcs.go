package main

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"log"
	"reflect"
	"runtime"
	"sync"
	"time"
)

func main() {
	t := NewT()

	// publish multicast set-config-item [section] [key] [value]
	t.OnMulticast("set-config-item", func(section, key, value string) {
		log.Printf("section: %v, key: %v, value: %v", section, key, value)
	})

	// request app-name get-stats
	t.OnPrivate("get-stats", func() string {
		return fmt.Sprintf("online-users: 10")
	})

	// round-robin: request caches get-url
	t.OnGroup("get-url", func(url string) string {
		// r := getUrl(a)
		// return r
		return "get-url: " + url
	})

	t.OnGroup("set-something", func(json string) string {
		return json
	})

	// connect to nats server
	err := t.Nats("nats://127.0.0.1:4222")
	if err != nil {
		panic(err)
	}

	t.SubMulticast("multicast")
	t.SubPrivate("app-name")
	t.SubGroup("crawlers", "groupname")

	runtime.Goexit()

	// t.call("cmd set-config-item 192.168.1.1")
	// t.pcall("cmd get-stats")

}

func NewT() T {
	var t T
	t.mfns = make(map[string]interface{})
	t.pfns = make(map[string]interface{})
	t.qfns = make(map[string]interface{})

	t.mu = &sync.RWMutex{}
	return t
}

type T struct {
	mfns map[string]interface{} // multicast
	pfns map[string]interface{} // private
	qfns map[string]interface{} // group queue
	nc   *nats.Conn
	mu   *sync.RWMutex
}

func (t *T) Nats(srv string) (err error) {
	t.nc, err = nats.Connect(srv, nats.DontRandomize(), nats.MaxReconnects(1<<31), nats.ReconnectWait(1*time.Second))
	if err != nil {
		return err
	}
	return nil
}

// group subscribe for pub/sub

func (t *T) SubMulticast(topic string) {
	t.nc.Subscribe(topic, func(msg *nats.Msg) {
		t.call(string(msg.Data))
	})
}

// subscribe for req/repl

func (t *T) SubPrivate(topic string) {
	t.nc.Subscribe(topic, func(msg *nats.Msg) {
		r := t.pcall(string(msg.Data))
		switch rv := r.(type) {
		case string:
			t.nc.Publish(msg.Reply, []byte(rv))
		case []byte:
			t.nc.Publish(msg.Reply, rv)
		}
	})
}

// group queue subscribe for req/repl

func (t *T) SubGroup(topic, q string) {
	t.nc.QueueSubscribe(topic, q, func(msg *nats.Msg) {
		r := t.qcall(string(msg.Data))
		switch kv := r.(type) {
		case string:
			t.nc.Publish(msg.Reply, []byte(kv))
		case []byte:
			t.nc.Publish(msg.Reply, kv)
		}

	})
}

func (t *T) call(s string) {
	cmd := pw(&s)

	t.mu.RLock()
	fn, ok := t.mfns[cmd]
	t.mu.RUnlock()
	if !ok {
		return
	}

	funcArgs := reflect.ValueOf(fn).Type().NumIn()
	funcValue := reflect.ValueOf(fn)

	v := make([]reflect.Value, funcArgs)
	for i := 0; i < funcArgs-1; i++ {
		v[i] = reflect.ValueOf(pw(&s))
	}
	if funcArgs > 0 {
      v[funcArgs-1] = reflect.ValueOf(s)
    }
	funcValue.Call(v)
}

func (t *T) qcall(s string) interface{} {
	cmd := pw(&s)

	t.mu.RLock()
	fn, ok := t.qfns[cmd]
	t.mu.RUnlock()
	if !ok {
		return "command not found"
	}

	funcArgs := reflect.ValueOf(fn).Type().NumIn()
	funcValue := reflect.ValueOf(fn)

	v := make([]reflect.Value, funcArgs)
	for i := 0; i < funcArgs-1; i++ {
		v[i] = reflect.ValueOf(pw(&s))
	}
	if funcArgs > 0 {
		v[funcArgs-1] = reflect.ValueOf(s)
	}
	return funcValue.Call(v)[0].Interface()
}

func (t *T) pcall(s string) interface{} {
	cmd := pw(&s)

	t.mu.RLock()
	fn, ok := t.pfns[cmd]
	t.mu.RUnlock()

	if !ok {
		return "command not found"
	}

	funcArgs := reflect.ValueOf(fn).Type().NumIn()
	funcValue := reflect.ValueOf(fn)

	v := make([]reflect.Value, funcArgs)
	for i := 0; i < funcArgs-1; i++ {
		v[i] = reflect.ValueOf(pw(&s))
	}
	if funcArgs > 0 {
		v[funcArgs-1] = reflect.ValueOf(s)
	}
	return funcValue.Call(v)[0].Interface()
}

func (t *T) OnMulticast(s string, f interface{}) {
	t.mu.Lock()
	t.mfns[s] = f
	t.mu.Unlock()
}

func (t *T) OnPrivate(s string, f interface{}) {
	t.mu.Lock()
	t.pfns[s] = f
	t.mu.Unlock()
}

func (t *T) OnGroup(s string, f interface{}) {
	t.mu.Lock()
	t.qfns[s] = f
	t.mu.Unlock()
}

func pw(s *string) string {
	l, rt, i := 100, "", 0

	for _, v := range *s {
		if v != ' ' {
			rt = rt + string(v)
		} else {
			break
		}
		i++
		if i >= l {
			break
		}
	}
	if len(*s) <= i {
		*s = ""
		return rt
	}
	*s = (*s)[i+1:]
	return rt
}

package main

import (
	"flag"
	"fmt"
	"github.com/nats-io/go-nats"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Node struct {
	Start   int64
	Updated int64
}

func (n *Node) IsAlive() bool {
	return time.Now().UnixNano()-n.Updated < time.Second.Nanoseconds()/10
}

type As struct {
	start   int64
	name    string
	master  bool
	alive   bool
	gotMsg  bool
	msgTime int64
	nodes   map[string]*Node
	mu      sync.Mutex
}

func (a *As) Set(name string, start int64) {
	a.mu.Lock()
	if _, ok := a.nodes[name]; !ok {
		a.nodes[name] = new(Node)
		a.nodes[name].Start = start
	}
	a.nodes[name].Updated = time.Now().UnixNano()
	a.mu.Unlock()
}

func (a *As) Update() {
	if !a.IsAlive() {
		return
	}
	start := as.start
	a.mu.Lock()
	for name, node := range a.nodes {
		if node.IsAlive() {
			if node.Start < start {
				start = node.Start
			}
		} else {
			delete(a.nodes, name)
		}
	}
	a.mu.Unlock()
	as.master = as.start <= start
}

func (a *As) IsMaster() bool {
	return as.master
}

func (a *As) IsSlave() bool {
	return !a.master
}
func (a *As) IsAlive() bool {
	return a.alive
}

var name = flag.String("n", "[noname]", "Service name")

var as *As

func main() {
	flag.Parse()
	as = new(As)
	as.nodes = make(map[string]*Node)
	start := time.Now().UnixNano()
	as.start = start
	as.name = *name

	cli, e := nats.Connect("nats://127.0.0.1:4222")
	if e != nil {
		panic(e)
	}
	as.alive = cli.IsConnected()

	go func() {
		for {
			cli.Publish("master-slave", []byte(fmt.Sprintf("%v %v %d", "alive", *name, start)))
			as.alive = cli.IsConnected()
			as.Set(*name, as.start)
			time.Sleep(30 * time.Millisecond)
		}
	}()
	go func() {
		time.Sleep(500 * time.Millisecond)
		if as.gotMsg == false && as.IsAlive() {
			as.master = true
		}
	}()
	go func() {
		for {
			time.Sleep(30 * time.Millisecond)
			as.Update()
		}
	}()

	cli.Subscribe("master-slave", func(msg *nats.Msg) {
		m := strings.Split(string(msg.Data), " ")
		if m[0] == "alive" && m[1] != *name {
			as.gotMsg = true
			as.msgTime = time.Now().UnixNano()
			num, _ := strconv.Atoi(m[2])
			as.Set(m[1], int64(num))
		}
	})

	for {
		fmt.Printf("%v\n", as.IsMaster())
		time.Sleep(100 * time.Millisecond)
	}

}

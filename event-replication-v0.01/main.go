package main

// TODO: queue downtimes

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/boltdb/bolt"
	"sync"
	// gnatsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

type BinLog struct {
	MsgID   int64
	MsgData string

	MsgParentID int64

	MsgCat  int64
	MsgProc string
}

func (bl *BinLog) encode() []byte {
	b, _ := json.Marshal(bl)
	return b
}
func (bl *BinLog) decode(v []byte) {
	json.Unmarshal(v, bl)
}

// Downtime Register Item
type dri struct {
	Start int64
	Now   int64
}

func (hi *dri) encode() []byte {
	b, _ := json.Marshal(hi)
	return b
}

func (hi *dri) decode(v []byte) {
	json.Unmarshal(v, hi)
}

var pmq *nats.Conn

var masterTopic string
var masterOfMasters string

var dbFile = flag.String("d", "", "-d a.db")
var natsServer = flag.String("ns", "nats://127.0.0.1:4222", "-ns nats://127.0.0.1:4222,nats://127.0.0.2:4222")

var shardID = flag.Int64("shard-id", 0, "--shard-id [0-99]")
var isMaster = flag.Bool("master", false, "--master")

var gtid []byte

var replicationFinished bool = false
var replicationState bool = true
var lastReplEvent int64 = time.Now().UnixNano()

var globalLock sync.Mutex

func init() {
	gtid = itob(time.Now().UnixNano())
}

var resyncCh chan struct{} = make(chan struct{})
var syncDone chan struct{} = make(chan struct{})

var inSync bool
var inSyncRemote string

var startSync chan string = make(chan string)

var db *bolt.DB
var lastID string

var lastMSG []byte

type callbacks []func([]byte)

func (cb *callbacks) Add(fn func([]byte)) {
	*cb = append(*cb, fn)
}

func (cb callbacks) Run(v []byte) {
	for _, c := range cb {
		c(v)
	}
}

var cbs callbacks

func OnEvent(v []byte) {
	// println("playing: " + string(v))
}

func OnEventReplay(msg BinLog) {
	// println("replaying: " + string(msg.encode()))
}

var appStarted bool

func OnSyncDone() {
	if appStarted {
		return
	}
	appStarted = true
	fmt.Println("start app")
	// startApp()
}

var syncedMaster string

func OnFoundSynced() {
	if appStarted {
		return
	}
	appStarted = true
	fmt.Println("start app2")
	// startApp()
}

/*
func startApp() {
	go func() {
		from := time.Now().UnixNano() - int64(90*time.Second)
		PlayAndSub(from, func(v []byte) {
			fmt.Printf("play %s\n", v)
		})
	}()
}
*/

func namespace(s string) string {
	namespace := "default"
	return fmt.Sprintf("%s-%s-", namespace, s)
}

var testwg *sync.WaitGroup = new(sync.WaitGroup)

func main() {

	flag.Parse()
	var err error

	pmq, err = nats.Connect(*natsServer, nats.DisconnectHandler(func(nc *nats.Conn) {
		panic("Disconnected")
	}))
	if err != nil {
		panic(err.Error())
	}

	db, _ = bolt.Open(*dbFile, 0755, nil)

	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("binlog"))
		tx.CreateBucketIfNotExists([]byte("m"))
		tx.CreateBucketIfNotExists([]byte("history"))
		tx.CreateBucketIfNotExists([]byte("masters"))
		return nil
	})

	db.Update(func(tx *bolt.Tx) error {
		bb := tx.Bucket([]byte("m"))
		mt := bb.Get([]byte("masterTopic"))
		if mt == nil {
			masterTopic = nuid.New().Next()
			bb.Put([]byte("masterTopic"), []byte(masterTopic))
		} else {
			masterTopic = string(mt)
		}
		return nil
	})

	var kv []byte
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("binlog"))
		c := b.Cursor()
		_, v := c.Last()
		kv = clone(v)
		lastID = fmt.Sprintf("%s", v)
		return nil
	})
	dr.Enque(kv)

	println("me: " + masterTopic)

	var ml sync.Mutex
	var cond = sync.NewCond(&ml)

	var needCounter int64
	var doneCounter int64

	go func() {
		for {
			master := <-startSync
			go func() {
				needCounter = int64(dr.Count()*mr.KnownMastersCount() - 1)
				_ = master

				dr.Sync(func(id, v []byte) {
					master := master

					var hi *dri = new(dri)
					hi.decode(v)

					playLog(master, hi.Now, hi.Start, func() {
						// doneCounter++
						atomic.AddInt64(&doneCounter, 1)
						cond.Broadcast()
					}, func() {
					})
				})
			}()
		}
	}()

	go func() {
		for {
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
			fmt.Printf("%v %v\n", needCounter, atomic.LoadInt64(&doneCounter))
			if needCounter == atomic.LoadInt64(&doneCounter) {
				dr.Clear()
				inSync = true
				OnSyncDone()
			}
		}
	}()

	pmq.Subscribe("broadcast", binlogWritter) // write to db

	pmq.Subscribe("control", func(msg *nats.Msg) {
		m := strings.SplitN(string(msg.Data), " ", 2)
		if m[0] == "set-insync" {
			dr.Clear()
			inSync = true
		}
		if m[0] == "reset-masters" {
			// create list of masters of now
			mr.ClearKnownMasters()
			dr.Clear()
			inSync = true
			pmq.Publish("control", []byte("master "+masterTopic))
			OnSyncDone()
		}
		if m[0] == "master" {
			mr.Register(m[1])
		}
	})

	pmq.Subscribe("synced-servers-"+masterTopic, func(msg *nats.Msg) {
		m := string(msg.Data)
		if sm.Set(m) {
			OnFoundSynced()
		}
	})

	// new node
	pmq.Subscribe("new-master", func(msg *nats.Msg) {
		// if not me
		m := string(msg.Data)
		if m != masterTopic {
			if *isMaster {
				if inSync {
					pmq.Publish("synced-servers-"+m, []byte(masterTopic))
				}
				pmq.Publish("master-"+m, []byte(masterTopic+" "+fmt.Sprintf("%v", inSync)))
				pmq.Publish("master-repl-"+m, []byte(masterTopic+" "+fmt.Sprintf("%v", inSync)))
			}
		}
	})

	//
	pmq.Subscribe("master-"+masterTopic, func(msg *nats.Msg) {
		masterOfMasters := strings.SplitN(string(msg.Data), " ", 2)
		if masterOfMasters[0] != masterTopic {
			if !mr.Check(masterOfMasters[0]) {
				mr.Register(masterOfMasters[0])
				pmq.Subscribe(masterOfMasters[0], binlogWritter)
			}
			if *isMaster {
				pmq.Publish("master-repl-"+masterOfMasters[0], []byte(masterTopic+" "+fmt.Sprintf("%v", inSync)))
			}

			if masterOfMasters[1] == "true" {
				if sm.Set(masterOfMasters[1]) {
					inSync = true
					OnFoundSynced()
				}
			}
		}
	})

	pmq.Subscribe("master-repl-"+masterTopic, func(msg *nats.Msg) {
		masterOfMasters := strings.SplitN(string(msg.Data), " ", 2)

		go func() {
			if masterTopic != masterOfMasters[0] {
				startSync <- masterOfMasters[0]
			}
		}()
	})

	pmq.Subscribe("slave-"+masterTopic, func(msg *nats.Msg) {

	})

	pmq.Subscribe("new-slave", func(msg *nats.Msg) {
		m := strings.SplitN(string(msg.Data), " ", 2)
		if m[0] != masterTopic {
			pmq.Publish("master-"+string(msg.Data), []byte(masterTopic+" "+fmt.Sprintf("%v", inSync)))
		}
	})

	pmq.Subscribe("replay-request-"+masterTopic, func(msg *nats.Msg) {
		args := strings.SplitN(string(msg.Data), " ", 2)

		var m BinLog
		json.Unmarshal([]byte(args[1]), &m)

		repl := fmt.Sprintf("replay-%s", args[0])

		var mm BinLog
		mm.MsgID = -1

		ms := binlogNext(m.MsgID)

		if ms != nil {
			pmq.Publish(repl, ms)
		} else {
			pmq.Publish(repl, mm.encode())
		}
	})

	if *isMaster {
		pmq.Publish("new-master", []byte(masterTopic))
	} else {
		pmq.Publish("new-slave", []byte(masterTopic))
	}

	// just for testing
	if *isMaster {
		go func() {
			for {
				publish("Your binlog message" + fmt.Sprintf("%d %d", time.Now().Unix(), *shardID))
				time.Sleep(1000 * time.Millisecond)
			}
		}()
	}

	runtime.Goexit()
}

func publish(msg string) {
	id := time.Now().UnixNano()
	id = id - (id % 100) + *shardID
	m := BinLog{id, msg, 0, 0, ""}

	pmq.Publish("broadcast", m.encode())
}

func request(topic string, msg BinLog) {
	server := mr.Current()
	if sm.Check() {
		server = sm.Name
	}
	ms := fmt.Sprintf("%s %s", topic, msg.encode())
	pmq.Publish("replay-request-"+server, []byte(ms))
}

func request2(topic, master string, msg BinLog) {
	ms := fmt.Sprintf("%s %s", topic, msg.encode())
	pmq.Publish("replay-request-"+master, []byte(ms))
}

func playAny(from, to int64, onMessage func(msg []byte), onDone func()) {
	chName := nuid.New().Next()

	var m BinLog

	ch := make(chan struct{})

	timer := time.NewTimer(1 * time.Second)

	var ns *nats.Subscription
	ns, _ = pmq.Subscribe("replay-"+chName, func(msg *nats.Msg) {
		m.decode(msg.Data)

		timer.Reset(1 * time.Second)

		if m.MsgID != -1 {
			onMessage([]byte(m.MsgData))
		}

		if from >= m.MsgID {
			request(chName, m)
		}

		if from <= m.MsgID || m.MsgID == -1 {
			ns.Unsubscribe()
			close(ch)
			go func() {
				onDone()
			}()
		}
	})
	m.MsgID = to
	request(chName, m)

	go func() {
		for {
			select {
			case <-timer.C:
				timer.Reset(1 * time.Second)
				mr.Next()
				request(chName, m)
			case <-ch:
				timer.Stop()
				return
			}
		}
	}()
}

func PlayAndSub(to int64, onMessage func(msg []byte)) {
	from := int64(1<<63 - 1)
	chName := nuid.New().Next()

	var m BinLog

	ch := make(chan struct{})

	timer := time.NewTimer(1 * time.Second)

	var ns *nats.Subscription
	ns, _ = pmq.Subscribe("replay-"+chName, func(msg *nats.Msg) {
		m.decode(msg.Data)
		timer.Reset(1 * time.Second)

		if m.MsgID != -1 {
			onMessage([]byte(m.MsgData))
		}

		if from >= m.MsgID {
			request(chName, m)
		}

		if from <= m.MsgID || m.MsgID == -1 {
			globalLock.Lock()
			if bytes.Compare(lastMSG, msg.Data) == 0 || m.MsgID == -1 {
				cbs.Add(onMessage)
			}
			globalLock.Unlock()

			ns.Unsubscribe()
			close(ch)
		}
	})
	m.MsgID = to
	request(chName, m)

	go func() {
		for {
			select {
			case <-timer.C:
				timer.Reset(1 * time.Second)
				mr.Next()
				request(chName, m)
			case <-ch:
				timer.Stop()
				return
			}
		}
	}()
}

func playLog(master string, from, to int64, onDone func(), onFail func()) {
	var m BinLog
	chName := nuid.New().Next()

	ch := make(chan struct{})

	timer := time.NewTimer(1 * time.Second)

	var numMessages int32

	var ns *nats.Subscription
	ns, _ = pmq.Subscribe("replay-"+chName, func(msg *nats.Msg) {
		m.decode(msg.Data)

		timer.Reset(1 * time.Second)
		var fn func()
		if m.MsgID != -1 {
			err := db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("binlog"))
				v := b.Get(itob(m.MsgID))
				if v == nil {
					fn = func() { OnEventReplay(m) }
				}
				return b.Put(itob(m.MsgID), msg.Data)
			})
			if err != nil {
				fmt.Printf("binlog-writter error: %v\n", err)
				return
			}

			atomic.AddInt32(&numMessages, 1)
		} else {
			if atomic.LoadInt32(&numMessages) == 0 {
				close(ch)
				go onFail()
				ns.Unsubscribe()
				return
			}
			close(ch)
			go onDone()
			ns.Unsubscribe()
			return
		}

		if fn != nil {
			fn()
		}

		if from >= m.MsgID && m.MsgID != -1 {
			request2(chName, master, m)
		}

		if from <= m.MsgID {
			if atomic.LoadInt32(&numMessages) == 0 {
				close(ch)
				go onFail()
				ns.Unsubscribe()
				return
			}
			close(ch)
			go onDone()
			ns.Unsubscribe()
		}
	})

	m.MsgID = to
	request2(chName, master, m)
	timer.Reset(1 * time.Second)
	go func() {
		for {
			select {
			case <-timer.C:
				request2(chName, master, m)
				timer.Reset(1 * time.Second)
			case <-ch:
				timer.Stop()
				return
			}
		}
	}()
}

func binlogWritter2(msg *nats.Msg) {
	var m BinLog
	m.decode(msg.Data)
	var fn func()

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("binlog"))
		v := b.Get(itob(m.MsgID))
		if v == nil {
			fn = func() { OnEventReplay(m) }
		}
		return b.Put(itob(m.MsgID), msg.Data)
	})
	if err != nil {
		fmt.Printf("binlog-writter error: %v\n", err)
		return
	}
	if fn != nil {
		fn()
	}
}

func binlogWritter(msg *nats.Msg) {

	globalLock.Lock()
	lastMSG = clone(msg.Data)
	globalLock.Unlock()

	var m BinLog
	m.decode(msg.Data)

	cbs.Run([]byte(m.MsgData))

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("binlog"))
		return b.Put(itob(m.MsgID), msg.Data)
	})

	if err != nil {
		fmt.Printf("binlog-writter error: %v\n", err)
		return
	}

	OnEvent(msg.Data)
}

func binlogNext(start int64) []byte {
	var rt []byte
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("binlog"))
		c := b.Cursor()
		c.Seek(itob(start))
		_, rt = c.Next()
		return nil
	})
	return rt
}

func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func chClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
	}
	return false
}

func NewMR() *MR {
	mr := new(MR)
	return mr
}

var mr *MR = NewMR()
var mr2 *MR = NewMR()

type MR struct {
	names []string
	cur   int
}

func (mr *MR) Len() int {
	return len(mr.names)
}

func (mr *MR) Next() {
	mr.cur++
	if mr.cur > len(mr.names)-1 {
		mr.cur = 0
	}
}

func (mr *MR) Current() string {
	if len(mr.names) == 0 {
		return masterTopic
	}
	return mr.names[mr.cur]
}

func (mr *MR) Check(name string) bool {
	for _, m := range mr.names {
		if m == name {
			return true
		}
	}
	return false
}

func (mr *MR) Register(name string) {
	if !mr.Check(name) {
		mr.names = append(mr.names, name)
	}
	db.Update(func(tx *bolt.Tx) error {
		tx.Bucket([]byte("masters")).Put([]byte(name), []byte(""))
		return nil
	})
}

func (mr *MR) KnownMastersCount() (r int) {
	db.View(func(tx *bolt.Tx) error {
		r = tx.Bucket([]byte("masters")).Stats().KeyN
		return nil
	})
	return
}

func (mr *MR) ClearKnownMasters() {
	db.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket([]byte("masters"))
		tx.CreateBucket([]byte("masters"))
		return nil
	})
}

var dr *DR = NewDR()

func NewDR() *DR {
	h := new(DR)
	return h
}

// Downtime Registrator
type DR struct{}

func (h *DR) Clear() {
	db.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket([]byte("history"))
		tx.CreateBucket([]byte("history"))
		return nil
	})
}

func (h *DR) Count() (r int) {
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("history"))
		r = b.Stats().KeyN
		return nil
	})
	return
}

func (h *DR) Enque(v []byte) {
	db.Update(func(tx *bolt.Tx) error {
		var b BinLog
		b.decode(v)

		var hi dri

		hi.Start = b.MsgID
		hi.Now = time.Now().UnixNano()

		hist, _ := tx.CreateBucketIfNotExists([]byte("history"))

		_num, _ := hist.NextSequence()
		num := itob(int64(_num))
		hist.Put(num, hi.encode())
		return nil
	})
}

func (h *DR) Sync(f func([]byte, []byte)) {
	db.View(func(tx *bolt.Tx) error {
		hist := tx.Bucket([]byte("history"))
		c := hist.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			f(clone(k), clone(v))
		}

		return nil
	})
}

func (h *DR) Delete(id []byte) {
	db.Update(func(tx *bolt.Tx) error {
		hist, _ := tx.CreateBucketIfNotExists([]byte("history"))
		return hist.Delete(id)
	})
}

func clone(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}

var sm *SM = &SM{""}

type SM struct {
	Name string
}

func (sm *SM) Check() bool {
	return sm.Name != ""
}

func (sm *SM) Set(v string) bool {
	if sm.Name == "" {
		sm.Name = v
		return true
	}
	return false
}

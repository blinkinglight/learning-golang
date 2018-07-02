package main

// TODO: queue downtimes

import (
	// "bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/boltdb/bolt"
	// gnatsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"runtime"
	"strings"
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

var pmq *nats.Conn

var masterTopic string
var masterOfMasters string

var dbFile = flag.String("d", "a.db", "-d a.db")
var natsServer = flag.String("ns", "nats://127.0.0.1:4222", "-ns nats://127.0.0.1:4222,nats://127.0.0.2:4222")

var serviceID = flag.Int64("service-id", 0, "--service-id [0-99]")
var isMaster = flag.Bool("master", false, "--master")

var gtid []byte

var replicationFinished bool = false
var replicationState bool = true
var lastReplEvent int64 = time.Now().UnixNano()

var firstMsg bool

func init() {
	gtid = itob(time.Now().UnixNano())
}

var resyncCh chan struct{} = make(chan struct{})

var db *bolt.DB
var lastID string
var lastMSG string

func main() {

	flag.Parse()
	var err error

	pmq, err = nats.Connect(*natsServer, nats.DisconnectHandler(func(nc *nats.Conn) {
		panic("Disconnected")
	}))
	if err != nil {
		panic(err.Error())
	}

	go func() {
		for {
			pmq.Publish("auto-discovery", []byte(masterTopic))
			time.Sleep(time.Second)
		}
	}()

	db, _ = bolt.Open(*dbFile, 0755, nil)

	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("binlog"))
		tx.CreateBucketIfNotExists([]byte("m"))
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

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("binlog"))
		c := b.Cursor()
		_, v := c.Last()

		lastID = fmt.Sprintf("%s", v)

		return nil
	})

	println("me: " + masterTopic)

	go func() {
		var m BinLog
		m.decode([]byte(lastID))
		<-resyncCh
		if mr.Len() > 0 {
			go playLog(time.Now().UnixNano(), m.MsgID, func() {
				fmt.Println("Sync Done")
				// remove from downtimes queue
			})
			return
		}
	}()

	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				// do something?
			case <-resyncCh:
				fmt.Println("Master found !")
				return
			}
		}
	}()

	pmq.Subscribe("new-master", func(msg *nats.Msg) {
		data := string(msg.Data)
		if !mr2.Check(data) {
			mr2.Register(data)
			pmq.Subscribe(data, binlogWritter)
		}
		// if not me
		if string(msg.Data) != masterTopic {
			pmq.Publish("master-"+string(msg.Data), []byte(masterTopic))
		}
	})

	pmq.Subscribe("master-"+masterTopic, func(msg *nats.Msg) {
		masterOfMasters = string(msg.Data)
		if masterOfMasters == masterTopic {
			return
		}
		if !mr.Check(masterOfMasters) {
			mr.Register(masterOfMasters)
			pmq.Subscribe(string(msg.Data), binlogWritter)
			if *isMaster {
				pmq.Publish("master-"+string(msg.Data), []byte(masterTopic))
			}
		}

		if !chClosed(resyncCh) {
			close(resyncCh)
		}
	})

	pmq.Subscribe("slave-"+masterTopic, func(msg *nats.Msg) {

	})

	pmq.Subscribe("new-slave", func(msg *nats.Msg) {
		if string(msg.Data) != masterTopic {
			pmq.Publish("slave-"+string(msg.Data), []byte(masterTopic))
			pmq.Publish("master-"+string(msg.Data), []byte(masterTopic))
		}
	})

	pmq.Subscribe("play-"+masterTopic, func(msg *nats.Msg) {
		// process message here
		// write to db or do your async action
		// download missing file or do something else with message

		// json unmarsh gtid <= msg.id { done }
		// println("playing: " + string(msg.Data))

	})

	pmq.Subscribe("play2-"+masterTopic, func(msg *nats.Msg) {
		// process message here
		// write to db or do your async action
		// download missing file or do something else with message

		// json unmarsh gtid <= msg.id { done }
		println("replaying: " + string(msg.Data))

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

	// write to binlog and process if need'ed

	if *isMaster {
		pmq.Publish("new-master", []byte(masterTopic))
	} else {
		pmq.Publish("new-slave", []byte(masterTopic))
	}

	// just for testing
	if *isMaster {
		go func() {
			for {
				publish("Your binlog message")
				time.Sleep(1000 * time.Millisecond)
			}
		}()
	}

	runtime.Goexit()
}

func publish(msg string) {
	id := time.Now().UnixNano()
	id = id - (id % 100) + *serviceID
	m := BinLog{id, msg, 0, 0, ""}

	pmq.Publish(masterTopic, m.encode())
}

func request(topic string, msg BinLog) {
	ms := fmt.Sprintf("%s %s", topic, msg.encode())
	pmq.Publish("replay-request-"+mr.Current(), []byte(ms))
}

func play(from, to int64, onMessage func(msg []byte), onDone func()) {
	<-resyncCh
	playAny(from, to, onMessage, onDone)
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

func playLog(from, to int64, onDone func()) {
	var m BinLog

	ch := make(chan struct{})

	timer := time.NewTimer(1 * time.Second)

	var ns *nats.Subscription
	ns, _ = pmq.Subscribe("replay-"+masterTopic, func(msg *nats.Msg) {
		m.decode(msg.Data)

		timer.Reset(1 * time.Second)

		if m.MsgID != -1 {
			err := db.Update(func(tx *bolt.Tx) error {
				b, _ := tx.CreateBucketIfNotExists([]byte("binlog"))
				return b.Put(itob(m.MsgID), msg.Data)
			})
			if err != nil {
				fmt.Printf("binlog-writter error: %v\n", err)
				return
			}
			pmq.Publish("play2-"+masterTopic, []byte(m.MsgData))
		}

		if from >= m.MsgID {
			request(masterTopic, m)
		}

		if from <= m.MsgID || m.MsgID == -1 {
			ns.Unsubscribe()
			close(ch)
			go onDone()
		}
	})
	m.MsgID = to
	request(masterTopic, m)
	go func() {
		for {
			select {
			case <-timer.C:
				timer.Reset(1 * time.Second)
				mr.Next()
				request(masterTopic, m)
			case <-ch:
				timer.Stop()
				return
			}
		}
	}()
}

func binlogWritter(msg *nats.Msg) {

	var m BinLog
	m.decode(msg.Data)

	if firstMsg == false {
		gtid = itob(m.MsgID)
		firstMsg = true
	}

	err := db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("binlog"))
		return b.Put(itob(m.MsgID), msg.Data)
	})
	if err != nil {
		fmt.Printf("binlog-writter error: %v\n", err)
		return
	}
	pmq.Publish("play-"+masterTopic, msg.Data)
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
}

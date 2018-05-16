package main

/*
	go run main.go -d db1.db -ns nats://192.168.100.50
	use "publish(string) method to binlog your message or write your own"

*/

import (
	"bytes"
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
}

var pmq *nats.Conn

var masterTopic string
var masterOfMasters string

var dbFile = flag.String("d", "a.db", "-d a.db")
var natsServer = flag.String("ns", "nats://127.0.0.1:4222", "-ns nats://127.0.0.1:4222,nats://127.0.0.2:4222")
var gtid []byte

var replicationFinished bool = false
var replicationState bool = true
var lastReplEvent int64 = time.Now().UnixNano()

var firstMsg bool

func init() {
	gtid = itob(time.Now().UnixNano())
}

var db *bolt.DB
var lastID string
var lastMSG string

var masterq []string

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
		fmt.Printf("doh: %s\n", mt)
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

	pmq.Subscribe("play-"+masterTopic, func(msg *nats.Msg) {
		if firstMsg == false {
			var m BinLog
			json.Unmarshal(msg.Data, &m)
			gtid = itob(m.MsgID)
			firstMsg = true
		}
		// process message here
		// write to db or do your async action
		// download missing file or do something else with message

		// json unmarsh gtid <= msg.id { done }
		println("playing: " + string(msg.Data))

	})

	pmq.Subscribe("replay-request-"+masterTopic, func(msg *nats.Msg) {
		args := strings.SplitN(string(msg.Data), " ", 2)

		var m BinLog
		json.Unmarshal([]byte(args[1]), &m)

		// fmt.Printf("requested reply: %s\n", args)

		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("binlog"))
			c := b.Cursor()
			for k, v := c.Seek(itob(m.MsgID)); k != nil; k, v = c.Next() {
				// time.Sleep(time.Second)
				pmq.Publish("replay-"+args[0], v)
			}
			return nil
		})
	})

	// write to binlog and process if need'ed
	pmq.Subscribe("replay-"+masterTopic, binlogWritter2)

	pmq.Subscribe("master-"+masterTopic, func(msg *nats.Msg) {
		masterOfMasters = string(msg.Data)
		pmq.Subscribe(string(msg.Data), binlogWritter)
	})

	pmq.Subscribe("new-master", func(msg *nats.Msg) {
		// println("new-master")
		pmq.Subscribe(string(msg.Data), binlogWritter)
		// if not me
		if string(msg.Data) != masterTopic {
			// TODO: ir repl enqueue msg
			if replicationState == true {
				masterq = append(masterq, "master-"+string(msg.Data))
			} else {
				pmq.Publish("master-"+string(msg.Data), []byte(masterTopic))
			}
		}
	})

	pmq.Publish("new-master", []byte(masterTopic))
	go func() {
		time.Sleep(time.Second)
		if masterOfMasters != "" {
			// println("request replay from: replay-" + masterOfMasters)
			msg := fmt.Sprintf("%s %s", masterTopic, lastID)
			pmq.Publish("replay-request-"+masterOfMasters, []byte(msg))
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second)
			if time.Duration(time.Now().UnixNano()-lastReplEvent)/time.Second > 3 && replicationFinished == false {
				if lastMSG == "" {
					replicationFinished = true
				}

				if replicationFinished == false {
					msg := fmt.Sprintf("%s %s", masterTopic, lastMSG)
					lastReplEvent = time.Now().UnixNano()
					// println("request replay again from: replay-" + masterOfMasters)
					pmq.Publish("replay-request-"+masterOfMasters, []byte(msg))
				}

			}
			if replicationFinished == true {
				// if replicationFinished == true {
				replicationState = false
				for _, v := range masterq {
					pmq.Publish(v, []byte(masterTopic))
				}
				masterq = nil
				return
				// }
			}
		}
	}()

	go func() {
		for {
			publish("Your binlog message")
			time.Sleep(1000 * time.Millisecond)
		}
	}()

	runtime.Goexit()
}

func publish(msg string) {
	m := BinLog{time.Now().UnixNano(), msg}
	b, _ := json.Marshal(m)

	pmq.Publish(masterTopic, b)
}

func binlogWritter(msg *nats.Msg) {

	pmq.Publish("play-"+masterTopic, msg.Data)

	var m BinLog

	json.Unmarshal(msg.Data, &m)

	// fmt.Printf("%v\n", m)

	db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("binlog"))
		b.Put(itob(m.MsgID), msg.Data)

		return nil
	})

}

func binlogWritter2(msg *nats.Msg) {

	var m BinLog

	json.Unmarshal(msg.Data, &m)
	lastMSG = fmt.Sprintf("%s", msg.Data)

	if bytes.Compare(gtid, itob(m.MsgID)) > 0 {
		replicationState = true
		replicationFinished = false
		lastReplEvent = time.Now().UnixNano()
		pmq.Publish("play-"+masterTopic, msg.Data)
	}
	if bytes.Compare(gtid, itob(m.MsgID)) == 0 {
		replicationState = false
		replicationFinished = true
	}
	// fmt.Printf("syncing %v\n", m)

	db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("binlog"))
		b.Put(itob(m.MsgID), msg.Data)
		return nil
	})

}

func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

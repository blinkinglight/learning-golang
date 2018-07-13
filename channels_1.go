package main

import (
	"runtime"
	"time"
)

func main() {
	ch := make(chan func(string), 100)

	go func() {
		for {
			select {
			case fn := <-ch:
				fn("payload")
				continue
			default:
				println("sleeping / break")
				time.Sleep(1 * time.Second)
			}

		}
	}()

	go func() {
		for {
			time.Sleep(2 * time.Second)
			ch <- func(s string) { println("a", s) }
			ch <- func(s string) { println("b", s) }
			ch <- func(s string) { println("c", s) }
			ch <- func(s string) { println("d", s) }

		}
	}()

	runtime.Goexit()
}

package main

import (
	"testing"
)

func BenchmarkChannelFn(b *testing.B) {
	ch := make(chan func(string), 100)

	fn := func(payload string) {
	FOR1:
		for {
			select {
			case fn := <-ch:
				fn(payload)
				continue
			default:
				break FOR1
			}
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn("test")
	}
}

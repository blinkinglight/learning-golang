package main

import (
	"fmt"
	"strings"
)

func main() {
	b := make(map[string]string)
	s := []byte("some long long log message with http://url.com/?and=query&string=true ip=127.0.0.1 ua=some user agent random-key=some random data")

	_ = b
	key := []byte("")
	value := []byte("")
	last := "msg"
	space := true
	for i, k := range s {
		if k == '?' {
			space = false
		}
		if k == ' ' {
			space = true
		}
		if k == '=' && space == false {
			key = append(key, k)
			continue
		}
		if k == '=' {
			space = false
			b[string(key)] = ""
			last = string(key)
			key = []byte("")
			value = []byte("")
			continue
		} else {
			key = append(key, k)
			if k == ' ' || i == len(s)-1 {
				value = append(value, key...)
				key = []byte("")
				b[last] = strings.Trim(string(value), " ")
			}
		}
	}

	fmt.Printf("%v\n", b)
	for k, v := range b {
		fmt.Printf("%s = %s\n", k, v)
	}
}

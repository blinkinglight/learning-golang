package main

import (
	"bufio"
	"fmt"
	"strings"
)

type Parts []string
type Params []Parts

func main() {

	var params Params

	scanner := bufio.NewScanner(strings.NewReader(lines))

	for scanner.Scan() {
		parts := parseLine(scanner.Text())
		params = append(params, parts)
		parts = []string{}
	}
	fmt.Printf("status: (%v), user agent: %+v\n", params[3][5], params[3][8])
}

func parseLine(l string) []string {
	line := []byte(l)
	var parts Parts
	wantQuote := false
	param := []byte{}
	for pos, k := range line {
		if k == '"' && wantQuote == false {
			wantQuote = true
			continue
		}
		if k == '"' && wantQuote == true {
			wantQuote = false
			continue
		}
		if k == '[' && wantQuote == false {
			wantQuote = true
			continue
		} else if k == ']' && wantQuote == true {
			wantQuote = false
			continue
		}

		if k == ' ' && wantQuote == true {
			param = append(param, k)
			continue
		}
		if k != ' ' && k != '[' && k != ']' && k != '"' {
			param = append(param, k)
		}
		if (k == ' ' && wantQuote == false) || (pos == len(line)-1 && wantQuote == false) {
			parts = append(parts, string(param))
			param = []byte{}
			continue
		}
	}
	parts = append(parts, string(param))
	param = []byte{}

	return parts
}

const lines = `168.1.128.50 - - [09/Nov/2017:04:03:06 +0200] "GET / HTTP/1.1" 200 3129 "-" "-"
169.54.244.89 - - [09/Nov/2017:07:50:26 +0200] "GET / HTTP/1.1" 200 3129 "-" "-"
168.1.128.76 - - [09/Nov/2017:09:44:10 +0200] "GET / HTTP/1.1" 200 3129 "-" "-"
66.240.236.119 - - [09/Nov/2017:22:43:12 +0200] "GET / HTTP/1.1" 200 3129 "-" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
66.240.236.119 - - [09/Nov/2017:22:43:32 +0200] "GET /sitemap.xml HTTP/1.1" 404 624 "-" "-"
66.240.236.119 - - [09/Nov/2017:22:43:58 +0200] "GET /favicon.ico HTTP/1.1" 404 372 "-" "python-requests/2.10.0"
`

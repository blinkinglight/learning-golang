package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"strings"
)

var ls map[string]struct{}

var flagFile = flag.String("f", "access.log", "-f /var/log/apache/access.log")
var flagList = flag.String("l", "search.txt", "-l ips.txt")

func main() {
	flag.Parse()

	ls = make(map[string]struct{})

	fileList, e := os.Open(*flagList)
	if e != nil {
		log.Fatalf("Error: %v", e)
	}

	scannerList := bufio.NewScanner(fileList)
	for scannerList.Scan() {
		line := strings.Trim(scannerList.Text(), "\n\t")
		if line == "" {
			continue
		}
		ls[line] = struct{}{}
	}

	fileLog, e := os.Open(*flagFile)
	if e != nil {
		log.Fatalf("Error: %v", e)
	}

	scanner := bufio.NewScanner(fileLog)
	for scanner.Scan() {
		line := strings.Trim(scanner.Text(), "\n\t")
		if MatchLine(line) {
			log.Printf("Found %v", line)
		}
	}
}

func MatchLine(line string) bool {
	for k, _ := range ls {
		if strings.Contains(line, k) {
			return true
		}
	}
	return false
}

/*
	usage: app -l ipaddresses.txt -f filetogrep
*/

package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/nats-io/nats"
	"os"
)

var natsServer = flag.String("s", "nats://127.0.0.1:4222", "nats server nats://127.0.0.1:4222")
var topic = flag.String("t", "MQ channel", "-t dev-host.apache-logs")

func main() {
	flag.Parse()

	for {
		cli, err := nats.Connect(*natsServer)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		for {
			line, _ := bufio.NewReader(os.Stdin).ReadBytes('\n')
			cli.Publish(*topic, line[0:len(line)-1])
			err = cli.Flush()
			if err != nil {
				fmt.Printf("error: %v\n", err)
				continue
			}
		}
	}
	os.Exit(1)

}

/*
stream apache logs to nats.io MQ. apache config line:

CustomLog "|/opt/tools/mq-publish -t hostname.apache_access_log -s nats://127.0.0.1:4222" combined
*/

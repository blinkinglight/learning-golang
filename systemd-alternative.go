package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/spx/llog"
	"github.com/spx/pubsub"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

var cfg_daemon = flag.Bool("d", false, "run app as a daemon with -d=true")

var cfg_socket = flag.String("s", "", "run app with -s app.sock")

var cfg_logfile = flag.String("l", "", "to log output run with -l app.log or -l - to log to sdtout. by default - no std logging")

// var cfg_daemon = flag.Bool("d", false, "-d=true")

var restartDelay int

var ps *pubsub.PubSub

type Writer struct {
	t string
}

func (w Writer) Write(data []byte) (int, error) {
	d := bytes.TrimRight(data, "\n")
	ps.Pub(log_sprintf(w.t, d), "main")
	return len(data), nil
}

func writter(c net.Conn) {
	ch := ps.Sub("main")
	defer ps.Unsub(ch, "main")
	for {
		msg := <-ch
		_, err := c.Write(msg.([]byte))
		if err != nil {
			llog.Debugf("write error: %v", err)
			return
		}
	}
}

func reader(c net.Conn) {
	for {
		line, e := bufio.NewReader(c).ReadString('\n')
		if e != nil {
			if e != io.EOF {
				llog.Debugf("reader error: %v", e)
				return
			} else {
				line = ""
				continue
			}
		}
		line = strings.TrimRight(line, "\n")

		switch line {
		case "kill":
			close(killch)
			cmd.Process.Kill()
			exit = true
		case "restart":
			close(killch)
			cmd.Process.Kill()
		}
	}
}

var cmd *exec.Cmd
var killch chan struct{}

var exit bool

func init() {
	flag.Parse()

	if *cfg_daemon == true && len(*cfg_socket) == 0 {
		llog.Debugf("daemon mode requires admin socket file")
		os.Exit(0)
	}

	daemon_mode()
}

func main() {

	l_args := os.Args
	l_found := false
	l_cmd := []string{}
	for _, l_arg := range l_args {
		if l_found == true {
			l_cmd = append(l_cmd, l_arg)
		}
		if l_arg == "--" {
			l_found = true
		}
	}

	// llog.Debugf("%v", l_cmd)

	// os.Exit(1)

	if len(*cfg_logfile) > 0 {
		if *cfg_logfile != "-" {
			stdout, _ := os.OpenFile(*cfg_logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

			syscall.Dup2(int(stdout.Fd()), 1)
			syscall.Dup2(int(stdout.Fd()), 2)
		}
	} else {
		stdout, _ := os.OpenFile("/dev/null", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

		syscall.Dup2(int(stdout.Fd()), 1)
		syscall.Dup2(int(stdout.Fd()), 2)
	}

	// llog.Debugf("listing %v", filepath.Base("/var/www/default/crashlog.log"))
	m, _ := filepath.Glob(*cfg_logfile + "*")
	for _, v := range m {
		// llog.Debugf("glob name: %v", path.Base(v))
	}
	// llog.Debugf("glob: %v", m)

	if true {
		go func() {
			ch := ps.Sub("main")
			defer ps.Unsub(ch, "main")
			for {
				msg := <-ch
				fmt.Printf("%s", msg)
			}
		}()
	}

	ps = pubsub.New(4)

	// os.Remove("./ciaas.sock")

	if len(*cfg_socket) > 0 {

		if _, ferr := os.Stat(*cfg_socket); !os.IsNotExist(ferr) {
			llog.Debugf("socket file exists")
			os.Exit(0)
			return
		}

		defer os.Remove(*cfg_socket)

		l, err := net.Listen("unix", *cfg_socket)
		if err != nil {
			llog.Errorf("net listen error: %v", err)
		}

		go func() {
			for {
				fd, err := l.Accept()
				if err != nil {
					panic("error accept " + err.Error())
				}
				go reader(fd)
				go writter(fd)
			}
		}()

	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		// sig is a ^C, handle it
		close(killch)
		cmd.Process.Kill()
		exit = true
		llog.Debugf("got ctrl+c. exiting...")
	}()

	err_wr := Writer{t: "std-err"}
	out_wr := Writer{t: "std-out"}

	for {
		killch = make(chan struct{})
		// cmd = exec.Command("./test.sh")
		cmd = exec.Command(l_cmd[0], l_cmd[1:]...)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			llog.Errorf("error stdout pipe: %v", err)
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			llog.Errorf("error stderr pipe: %v", err)
		}

		err = cmd.Start()
		if err != nil {
			llog.Errorf("error start cmd: %v", err)
		}

		go func() {
			for {
				select {
				case <-killch:
					break
				default:
					io.Copy(out_wr, stdout)
				}
			}
		}()
		go func() {
			for {
				select {
				case <-killch:
					break
				default:
					io.Copy(err_wr, stderr)
				}
			}
		}()
		cmd.Wait()
		if exit == true {
			llog.Debugf("Exiting...")
			break
		}
		time.Sleep(time.Duration(restartDelay) * time.Millisecond)
		llog.Debugf("restarting subapp")
	}

}

func log_sprintf(t string, msg []byte) []byte {
	r := fmt.Sprintf(
		"[%v] (%s) %s\n",
		t,
		time.Now().Format("2006/01/02-15:04:05"),
		msg,
	)
	return []byte(r)
}

func daemon_mode() {
	if *cfg_daemon {
		args := os.Args[1:]
		i := 0
		for ; i < len(args); i++ {
			if args[i] == "-d=true" {
				args[i] = "-d=false"
				break
			}
		}
		cmd := exec.Command(os.Args[0], args...)
		cmd.Start()
		fmt.Println("[PID]", cmd.Process.Pid)
		os.Exit(0)
	}
}


/*
app usage:
main -s app.sock -- ./this.sh is your app with params
socat - UNIX-CONNECT:app.sock        realtime stdout/stderror/log tracking
commands to send: restart, kill
*/
package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Process struct {
	pid     int
	loop    bool
	cmd     string
	rcmd    *exec.Cmd
	running bool
}

func (p *Process) Start() {
	if p.running == true {
		fmt.Printf("Process %d already running\n> ", p.pid)
		return
	}
	p.loop = true
	for p.loop {
		fmt.Printf("Starting [pid] %d, cmd: %v\n> ", p.pid, p.cmd)
		p.rcmd = exec.Command(p.cmd)
		p.rcmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		// p.rcmd.Stdout = os.Stdout
		if e := p.rcmd.Start(); e == nil {
			p.running = true
			p.rcmd.Wait()
			p.running = false
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func (p *Process) Restart() {
	fmt.Printf("Restarting %d\n", p.pid)
	syscall.Kill(-p.rcmd.Process.Pid, syscall.SIGKILL)
}

func (p *Process) Stop() {
	fmt.Printf("Stoping %d\n", p.pid)
	p.loop = false
	syscall.Kill(-p.rcmd.Process.Pid, syscall.SIGKILL)
}

type Processes []*Process

func (ps *Processes) Add(p *Process) {
	*ps = append(*ps, p)
}

func (ps *Processes) Remove(pid int) {
	pss := Processes{}
	for _, proc := range *ps {
		if proc.pid != pid {
			pss = append(pss, proc)
		} else {
			proc.Stop()
		}
	}
	*ps = pss
}

func (ps *Processes) StopAll() {
	for _, p := range *ps {
		p.Stop()
	}
}

var ps Processes = Processes{}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %v /path/to/shell/scripts", os.Args[0])
		os.Exit(1)
	}
	stat, err := os.Stat(os.Args[1])
	if os.IsNotExist(err) || !stat.IsDir() {
		fmt.Printf("Directory doesnt exists: %v\n", os.Args[1])
		os.Exit(1)
	}

	defer func() {
		if r := recover(); r != nil {
			ps.StopAll()
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGSTOP)
	go func() {
		<-c
		signal.Stop(c)
		ps.StopAll()
		os.Exit(0)
	}()

	n := 1
	filepath.Walk(os.Args[1], func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		p := new(Process)
		p.pid = n
		p.cmd = path
		p.loop = true
		go p.Start()
		ps.Add(p)
		n++
		return nil
	})

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("> ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimRight(text, "\n")
		cmd := strings.SplitN(text, " ", 2)
		// fmt.Printf("%+q %+v\n", cmd, cmd)
		switch cmd[0] {
		case "ls":
			for _, proc := range ps {
				fmt.Printf("pid: %d, cmd: %v, running: %v\n", proc.pid, proc.cmd, proc.running)
			}
		case "remove":
			if len(cmd) < 2 {
				fmt.Println("usage: stop [pid]")
				continue
			}
			pid, _ := strconv.Atoi(cmd[1])
			ps.Remove(pid)
		case "add":
			if len(cmd) < 2 {
				fmt.Println("usage: stop [pid]")
				continue
			}
			_, err := os.Stat(cmd[1])
			if os.IsNotExist(err) {
				fmt.Println("File not found")
				continue
			}
			p := new(Process)
			p.pid = n
			p.cmd = cmd[1]
			p.loop = true
			go p.Start()
			ps.Add(p)
			n++

		case "stop":
			if len(cmd) < 2 {
				fmt.Println("usage: stop [pid]")
				continue
			}
			pid, _ := strconv.Atoi(cmd[1])
			for _, proc := range ps {
				if proc.pid == pid {
					proc.Stop()
				}
			}
		case "start":
			if len(cmd) < 2 {
				fmt.Println("usage: start [pid]")
				continue
			}
			pid, _ := strconv.Atoi(cmd[1])
			for _, proc := range ps {
				if proc.pid == pid {
					go proc.Start()
				}
			}
		case "restart":
			if len(cmd) < 2 {
				fmt.Println("usage: restart [pid]")
				continue
			}
			if cmd[1] == "all" {
				for _, proc := range ps {
					proc.Restart()
				}
			} else {
				pid, _ := strconv.Atoi(cmd[1])
				for _, proc := range ps {
					if proc.pid == pid {
						go proc.Restart()
					}
				}
			}
		case "status":
			if len(cmd) < 2 {
				fmt.Println("usage: status [pid]")
				continue
			}
			pid, _ := strconv.Atoi(cmd[1])
			for _, proc := range ps {
				if proc.pid == pid {
					fmt.Printf("pid: %d, cmd: %v, running: %v\n", proc.pid, proc.cmd, proc.running)
				}
			}
		case "help":
			fmt.Println("stop [pid]")
			fmt.Println("start [pid]")
			fmt.Println("status [pid]")
			fmt.Println("restart [pid]")
			fmt.Println("exit")
		case "exit":
			ps.StopAll()
			os.Exit(0)
		}
	}
}


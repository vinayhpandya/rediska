package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"rediska/commands"
	"rediska/core"
)

type CommandRequest struct {
	cmd     commands.Command
	replych chan []byte
}

func Run(host string, port int) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	listener, error := net.Listen("tcp", addr)
	if error != nil {
		log.Fatalf("Error connecting to host %v \n", host)
		return fmt.Errorf("listen on %s: %w", addr, error)
	}
	defer listener.Close()

	commandCh := make(chan CommandRequest, 256)

	go dispatchWorker(commandCh)

	fmt.Printf("rediska is listening on %s \n", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v \n", err)
			continue
		}
		go handleConnection(conn, commandCh)
	}
}

func dispatchWorker(commandCh <-chan CommandRequest) {
	for req := range commandCh {
		reply := commands.Dispatch(&req.cmd)
		req.replych <- reply
	}
}
func handleConnection(conn net.Conn, commandCh chan<- CommandRequest) {
	defer conn.Close()
	log.Printf("client connected: %s", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	for {
		value, err := core.Decode(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("read error from %s: %v \n", conn.RemoteAddr(), err)
			}
			log.Printf("client disconnected: %s \n", conn.RemoteAddr())
			return
		}
		command, err := commands.ParseCommand(value)
		if err != nil {
			log.Printf("parse error from %s: %v", conn.RemoteAddr(), err)
			if _, werr := conn.Write(core.EncodeError("ERR " + err.Error())); werr != nil {
				log.Printf("write error to %s: %v", conn.RemoteAddr(), werr)
				return
			}
			continue
		}
		log.Printf("received command: %s args=%v", command.Name, command.Args)
		replych := make(chan []byte, 1)
		commandCh <- CommandRequest{
			cmd:     *command,
			replych: replych,
		}
		reply := <-replych
		log.Printf("reply: %q", string(reply))

		if _, err := conn.Write(reply); err != nil {
			log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
			return
		}
	}
}

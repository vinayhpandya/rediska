package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"rediska/commands"
	"rediska/core"
	"strings"
	"time"
)

type CommandRequest struct {
	cmd     commands.Command
	replych chan []byte
}

func Run(host string, port int, appendOnly bool, appendFilename string) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	fmt.Println("Append file name is ", appendFilename)
	if appendOnly {
		fmt.Println("Running rediska in append only file mode")
		aof, error := core.NewAOF(appendFilename)
		if error != nil {
			return fmt.Errorf("init AOF: %w", error)
		}
		defer aof.Close()
		commands.SetAOF(aof)
		log.Println("Recovering from appendOnly file")
		if err := aof.Recover(func(args []string) {
			if len(args) == 0 {
				return
			}
			cmd := &commands.Command{
				Name: strings.ToUpper(args[0]),
				Args: args[1:],
			}
			commands.Dispatch(cmd)
		}); err != nil {
			return fmt.Errorf("Error during AOF recovery: %w", err)
		}
	}
	listener, error := net.Listen("tcp", addr)
	if error != nil {
		log.Fatalf("Error connecting to host %v \n", host)
		return fmt.Errorf("listen on %s: %w", addr, error)
	}
	defer listener.Close()

	commandCh := make(chan CommandRequest, 256)

	go dispatchWorker(commandCh)

	go startActiveExpiry(commandCh)
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

func startActiveExpiry(commandCh chan<- CommandRequest) {
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		for {
			replyCh := make(chan []byte, 1)
			commandCh <- CommandRequest{
				cmd:     commands.Command{Name: "_EXPIRY"},
				replych: replyCh,
			}
			reply := <-replyCh
			deletedKeys, err := core.DecodeInteger(reply)
			if err != nil {
				break
			}

			if deletedKeys < 5 {
				break
			}

		}
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
	writer := bufio.NewWriter(conn)
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
		if _, err := writer.Write(reply); err != nil {
			log.Printf("Writing")
			log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
			return
		}
		if reader.Buffered() == 0 {
			log.Printf("Flushing")
			if err := writer.Flush(); err != nil {
				log.Printf("flush error to %s: %v \n", conn.RemoteAddr(), err)
				return
			}
		}
	}
}

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

func Run(host string, port int) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	listener, error := net.Listen("tcp", addr)
	if error != nil {
		log.Fatalf("Error connecting to host %v \n", host)
		return fmt.Errorf("listen on %s: %w", addr, error)
	}
	defer listener.Close()
	fmt.Printf("rediska is listening on %s \n", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v \n", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
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
		cmd, err := commands.ParseCommand(value)
		if err != nil {
			log.Printf("parse error from %s: %v", conn.RemoteAddr(), err)
			if _, werr := conn.Write(core.EncodeError("ERR " + err.Error())); werr != nil {
				log.Printf("write error to %s: %v", conn.RemoteAddr(), werr)
				return
			}
			continue
		}

		log.Printf("received command: %s args=%v", cmd.Name, cmd.Args)

		reply := commands.Dispatch(cmd)

		log.Printf("reply: %q", string(reply))

		if _, err := conn.Write(reply); err != nil {
			log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
			return
		}
	}
}

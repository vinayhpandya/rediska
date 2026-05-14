package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"rediska/core"
)

func Run(host string, port int) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	listener, error := net.Listen("tcp", addr)
	if error != nil {
		log.Fatal("Error connecting to host %v \n", host)
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
		log.Printf("received from %s: %#v", conn.RemoteAddr(), value)
		if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
			log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
			return
		}
	}
}

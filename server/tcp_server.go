package server

import (
	"fmt"
	"io"
	"log"
	"net"
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
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("read error from %s: %v \n", conn.RemoteAddr(), err)
			}
			log.Printf("client disconnected: %s \n", conn.RemoteAddr())
			return
		}
		log.Printf("received from %s (%d bytes): %q \n", conn.RemoteAddr(), n, buf[:n])
		if _, err := conn.Write(buf[:n]); err != nil {
			log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
			return
		}
	}
}

package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"rediska/commands"
	"rediska/core"
	"strings"
	"sync"
	"time"
)

type CommandRequest struct {
	cmd     commands.Command
	replych chan []byte
}

const shutdownPollInterval = 500 * time.Millisecond

func Run(ctx context.Context, host string, port int, appendOnly bool, appendFilename string) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	var aof *core.AOF
	fmt.Println("Append file name is ", appendFilename)
	if appendOnly {
		fmt.Println("Running rediska in append only file mode")
		a, err := core.NewAOF(appendFilename)
		if err != nil {
			return fmt.Errorf("init AOF: %w", err)
		}
		aof = a
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
			aof.Close()
			return fmt.Errorf("Error during AOF recovery: %w", err)
		}
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		if aof != nil {
			aof.Close()
		}
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	go func() {
		<-ctx.Done()
		log.Println("shutdown signal received")
		listener.Close()
	}()

	commandCh := make(chan CommandRequest, 256)
	workerdone := make(chan struct{})

	go func() {
		dispatchWorker(commandCh)
		close(workerdone)
	}()

	var producers sync.WaitGroup
	producers.Add(1)
	go startActiveExpiry(ctx, commandCh, &producers)
	fmt.Printf("rediska is listening on %s \n", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("accept error: %v \n", err)

			continue
		}
		producers.Add(1)
		go func(c net.Conn) {
			defer producers.Done()
			handleConnection(ctx, c, commandCh)
		}(conn)
		// go handleConnection(conn, commandCh)
	}
	// Shutdown procedure
	producers.Wait()
	close(commandCh)
	<-workerdone
	log.Println("command pipeline drained")

	if aof != nil {
		log.Println("flushing append-only file to disk (fsync)")
		if err := aof.Sync(); err != nil {
			log.Printf("AOF sync error: %v", err)
		}
		if err := aof.Close(); err != nil {
			log.Printf("AOF close error: %v", err)
		}
	}
	log.Println("shutdown complete")
	return nil

}

func startActiveExpiry(ctx context.Context, commandCh chan<- CommandRequest, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				replyCh := make(chan []byte, 1)
				select {
				case commandCh <- CommandRequest{
					cmd:     commands.Command{Name: "_EXPIRY"},
					replych: replyCh,
				}:
				case <-ctx.Done():
					return
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
}

func dispatchWorker(commandCh <-chan CommandRequest) {
	for req := range commandCh {
		reply := commands.Dispatch(&req.cmd)
		req.replych <- reply
	}
}
func handleConnection(ctx context.Context, conn net.Conn, commandCh chan<- CommandRequest) {
	defer conn.Close()
	log.Printf("client connected: %s", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		if ctx.Err() != nil {
			return
		}
		conn.SetReadDeadline(time.Now().Add(shutdownPollInterval))
		value, err := core.Decode(reader)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue // our 500ms poll fired, no command yet — loop back (and re-check ctx at top)
			}
			if err != io.EOF {
				log.Printf("read error from %s: %v", conn.RemoteAddr(), err)
			}
			log.Printf("client disconnected: %s", conn.RemoteAddr())
			return
		}
		conn.SetReadDeadline(time.Time{})
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

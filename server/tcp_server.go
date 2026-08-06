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
	cmd      commands.Command
	batch    []*commands.Command // MULTI/EXEC: dispatched as one array-wrapped reply
	pipeline []*commands.Command // pipelined commands: dispatched as concatenated individual replies
	replych  chan []byte
}

// replyChPool reuses reply channels across commands instead of allocating a
// fresh one per command. Each channel is used for exactly one send/receive
// cycle (buffered, capacity 1) and is guaranteed drained before it's
// returned to the pool, so it's always safe to hand out again.
var replyChPool = sync.Pool{
	New: func() any {
		return make(chan []byte, 1)
	},
}

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
		var reply []byte
		switch {
		case req.batch != nil:
			reply = commands.DispatchBatch(req.batch)
		case req.pipeline != nil:
			reply = commands.DispatchPipeline(req.pipeline)
		default:
			reply = commands.Dispatch(&req.cmd)
		}
		req.replych <- reply
	}
}
func handleConnection(ctx context.Context, conn net.Conn, commandCh chan<- CommandRequest) {
	defer conn.Close()
	log.Printf("client connected: %s", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	decoder := core.NewDecoder(reader)
	tx := newTxState()

	// Instead of polling ctx.Err() every loop and re-arming a read deadline
	// on every command (two syscalls per command just for shutdown support),
	// one goroutine waits on ctx and closes the conn directly when it fires.
	// That immediately unblocks the in-flight core.Decode read with an error,
	// so the loop below exits on its own — no polling needed on the hot path.
	watcherDone := make(chan struct{})
	defer close(watcherDone)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-watcherDone:
		}
	}()

	// dispatchSingle sends one command through commandCh and writes its
	// reply -- the same fast path as before, no extra allocation beyond
	// what already existed. Used both for the common non-pipelined case
	// and as dispatchPipeline's fallback when a "pipeline" turns out to
	// only have one command in it.
	dispatchSingle := func(cmd *commands.Command) error {
		replych := replyChPool.Get().(chan []byte)
		commandCh <- CommandRequest{cmd: *cmd, replych: replych}
		reply := <-replych
		replyChPool.Put(replych)
		_, err := writer.Write(reply)
		return err
	}

	// dispatchPipeline sends a batch of plain commands through commandCh as
	// ONE request, so a client that pipelines many commands back-to-back
	// (redis-benchmark -P, or any RESP client pipelining) pays for a single
	// channel round-trip instead of one per command. Replies come back
	// concatenated, exactly as if each had been sent individually.
	dispatchPipeline := func(pipeline []*commands.Command) error {
		if len(pipeline) == 1 {
			return dispatchSingle(pipeline[0])
		}
		replych := replyChPool.Get().(chan []byte)
		commandCh <- CommandRequest{pipeline: pipeline, replych: replych}
		reply := <-replych
		replyChPool.Put(replych)
		_, err := writer.Write(reply)
		return err
	}

	// handleAction writes the reply for a non-passthrough action (a
	// transaction-control reply, or an EXEC's batch dispatch). Factored out
	// so it can be called both for the "current" command and for a command
	// discovered while accumulating a pipeline (see below).
	handleAction := func(action txAction, txReply []byte, batch []*commands.Command) error {
		switch action {
		case actionReply:
			_, err := writer.Write(txReply)
			return err
		case actionExec:
			replych := replyChPool.Get().(chan []byte)
			commandCh <- CommandRequest{batch: batch, replych: replych}
			reply := <-replych
			replyChPool.Put(replych)
			_, err := writer.Write(reply)
			return err
		}
		return nil
	}

	for {
		value, err := decoder.Decode()
		if err != nil {
			if err != io.EOF && ctx.Err() == nil {
				log.Printf("read error from %s: %v", conn.RemoteAddr(), err)
			}
			log.Printf("client disconnected: %s", conn.RemoteAddr())
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
		action, txReply, batch := tx.handle(command)

		if action != actionPassthrough {
			if err := handleAction(action, txReply, batch); err != nil {
				log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
				return
			}
		} else {
			// Fold in any further commands the client already sent without
			// waiting for a reply (pipelining) -- reader.Buffered() > 0
			// means more RESP data is already sitting in memory, so
			// decoding it here doesn't block. Each is still run through
			// tx.handle in order, exactly as if handled one at a time;
			// only genuine plain commands get added to the batch.
			pipeline := []*commands.Command{command}
		pipelineLoop:
			for reader.Buffered() > 0 {
				v, err := decoder.Decode()
				if err != nil {
					// A raw protocol decode error is fatal for the
					// connection -- same as the top-level decode error
					// path above. Flush whatever valid replies we've
					// already accumulated first so they aren't lost, then
					// disconnect. (Silently dropping the bad frame and
					// continuing would desync a pipelining client's
					// request/reply counting -- it would never learn that
					// one of its requests got no reply.)
					if flushErr := dispatchPipeline(pipeline); flushErr != nil {
						log.Printf("write error to %s: %v \n", conn.RemoteAddr(), flushErr)
						return
					}
					if flushErr := writer.Flush(); flushErr != nil {
						log.Printf("flush error to %s: %v \n", conn.RemoteAddr(), flushErr)
						return
					}
					if err != io.EOF && ctx.Err() == nil {
						log.Printf("read error from %s: %v", conn.RemoteAddr(), err)
					}
					log.Printf("client disconnected: %s", conn.RemoteAddr())
					return
				}
				nextCmd, perr := commands.ParseCommand(v)
				if perr != nil {
					if err := dispatchPipeline(pipeline); err != nil {
						log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
						return
					}
					pipeline = nil
					log.Printf("parse error from %s: %v", conn.RemoteAddr(), perr)
					if _, werr := conn.Write(core.EncodeError("ERR " + perr.Error())); werr != nil {
						log.Printf("write error to %s: %v", conn.RemoteAddr(), werr)
						return
					}
					break pipelineLoop
				}
				nextAction, nextTxReply, nextBatch := tx.handle(nextCmd)
				if nextAction != actionPassthrough {
					// Flush what's accumulated so far first, to preserve
					// reply order, then handle this one normally.
					if err := dispatchPipeline(pipeline); err != nil {
						log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
						return
					}
					pipeline = nil
					if err := handleAction(nextAction, nextTxReply, nextBatch); err != nil {
						log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
						return
					}
					break pipelineLoop
				}
				pipeline = append(pipeline, nextCmd)
			}
			if pipeline != nil {
				if err := dispatchPipeline(pipeline); err != nil {
					log.Printf("write error to %s: %v \n", conn.RemoteAddr(), err)
					return
				}
			}
		}
		if reader.Buffered() == 0 {
			if err := writer.Flush(); err != nil {
				log.Printf("flush error to %s: %v \n", conn.RemoteAddr(), err)
				return
			}
		}
	}
}

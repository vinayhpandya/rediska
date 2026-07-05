package server

import (
	"fmt"
	"rediska/commands"
	"rediska/core"
)

type txState struct {
	isMulti bool
	queued  []*commands.Command
	dirty   bool
}

type txAction int

const (
	// actionPassthrough: not a transaction command, not queuing — proceed
	// with the normal commandCh send/receive path, unchanged.
	actionPassthrough txAction = iota
	// actionReply: a reply has already been decided (OK, QUEUED, or an
	// error). Caller should just write `reply` to the client — nothing
	// goes to commandCh.
	actionReply
	// actionExec: caller should submit `batch` to commandCh as a single
	// CommandRequest, so all queued commands run as one atomic unit.
	actionExec
)

func newTxState() *txState {
	return &txState{}
}

func (t *txState) reset() {
	t.isMulti = false
	t.queued = nil
	t.dirty = false
}

func (t *txState) handle(cmd *commands.Command) (action txAction, reply []byte, batch []*commands.Command) {
	switch cmd.Name {
	case "MULTI":
		if t.isMulti {
			return actionReply, core.EncodeError("ERR MULTI calls can not be nested"), nil
		}
		t.isMulti = true
		return actionReply, core.EncodeSimpleString("OK"), nil
	case "DISCARD":
		if !t.isMulti {
			return actionReply, core.EncodeError("ERR DISCARD without MULTI"), nil
		}
		t.reset()
		return actionReply, core.EncodeSimpleString("OK"), nil
	case "EXEC":
		if !t.isMulti {
			return actionReply, core.EncodeError("ERR EXEC without MULTI"), nil
		}
		if t.dirty {
			t.reset()
			return actionReply, core.EncodeError("EXECABORT Transaction discarded because of previous errors."), nil
		}
		queued := t.queued
		t.reset()
		return actionExec, nil, queued
	default:
		if !t.isMulti {
			return actionPassthrough, nil, nil
		}
		if !commands.IsKnownCommand(cmd.Name) {
			t.dirty = true
			return actionReply, core.EncodeError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name)), nil
		}
		t.queued = append(t.queued, cmd)
		return actionReply, core.EncodeSimpleString("QUEUED"), nil
	}
}

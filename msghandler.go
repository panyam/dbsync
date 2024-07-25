package dbsync

import (
	"errors"
	"log"

	"github.com/jackc/pglogrepl"
)

var ErrStopProcessingMessages = errors.New("message processing halted")

// Type of the callback function to handle messages read (peeked) from replication slot before the offset is forwarded.
// The callback accepts the list of messages (at the head of the replication slot) and returns two outputs:
//   - numProcessed - The number of messages processed.  The poller then forwards the slot's by this amount (instead of how many it originally peeked).
//   - stop - A bool variable that signals to the poller if it should stop processing any further messages from the replication slot.
type EventsProcessor func([]PGMSG, error) (numProcessed int, stop bool)

type Batcher interface {
	BatchUpsert(doctype string, docs []StringMap) (any, error)
	BatchDelete(doctype string, docids []string) (any, error)
}

type PGMSG struct {
	LSN  string
	Xid  uint64
	Data []byte
}

// Handles different kinds of messages from the replication slot
type MessageHandler interface {
	HandleRefresh(key any, value any) error
	HandleBeginMessage(idx int, msg *pglogrepl.BeginMessage) error
	HandleCommitMessage(idx int, msg *pglogrepl.CommitMessage) error
	HandleRelationMessage(idx int, msg *pglogrepl.RelationMessage, tableInfo *PGTableInfo) error
	HandleUpdateMessage(idx int, msg *pglogrepl.UpdateMessage, reln *pglogrepl.RelationMessage, tableInfo *PGTableInfo) error
	HandleDeleteMessage(idx int, msg *pglogrepl.DeleteMessage, reln *pglogrepl.RelationMessage, tableInfo *PGTableInfo) error
	HandleInsertMessage(idx int, msg *pglogrepl.InsertMessage, reln *pglogrepl.RelationMessage, tableInfo *PGTableInfo) error
}

// A default implementation of the messagge handler with no-op methods
type DefaultMessageHandler struct {
}

func (d *DefaultMessageHandler) HandleRefresh(key any, value any) (err error) {
	return
}

func (d *DefaultMessageHandler) HandleBeginMessage(idx int, msg *pglogrepl.BeginMessage) (err error) {
	return
}

func (d *DefaultMessageHandler) HandleCommitMessage(idx int, msg *pglogrepl.CommitMessage) (err error) {
	return
}
func (d *DefaultMessageHandler) HandleRelationMessage(idx int, msg *pglogrepl.RelationMessage, tableInfo *PGTableInfo) (err error) {
	return
}
func (d *DefaultMessageHandler) HandleUpdateMessage(idx int, msg *pglogrepl.UpdateMessage, reln *pglogrepl.RelationMessage, tableInfo *PGTableInfo) (err error) {
	return
}
func (d *DefaultMessageHandler) HandleDeleteMessage(idx int, msg *pglogrepl.DeleteMessage, reln *pglogrepl.RelationMessage, tableInfo *PGTableInfo) (err error) {
	return
}
func (d *DefaultMessageHandler) HandleInsertMessage(idx int, msg *pglogrepl.InsertMessage, reln *pglogrepl.RelationMessage, tableInfo *PGTableInfo) (err error) {
	return
}

// A message handler that prints events to stdout
type EchoMessageHandler struct {
	DefaultMessageHandler
}

func (d *EchoMessageHandler) HandleRefresh(key any, value any) (err error) {
	log.Println("HandleRefresh: ", key, value)
	return
}

func (d *EchoMessageHandler) HandleBeginMessage(idx int, msg *pglogrepl.BeginMessage) (err error) {
	log.Println("HandleBeginMessage: ", idx, msg)
	return
}

func (d *EchoMessageHandler) HandleCommitMessage(idx int, msg *pglogrepl.CommitMessage) (err error) {
	log.Println("HandleCommitMessage: ", idx, msg)
	return
}
func (d *EchoMessageHandler) HandleRelationMessage(idx int, msg *pglogrepl.RelationMessage, tableInfo *PGTableInfo) (err error) {
	log.Println("HandleRelationMessage: ", idx, msg, tableInfo)
	return
}
func (d *EchoMessageHandler) HandleUpdateMessage(idx int, msg *pglogrepl.UpdateMessage, reln *pglogrepl.RelationMessage, tableInfo *PGTableInfo) (err error) {
	log.Println("HandleUpdateMessage: ", idx, msg, reln, tableInfo)
	return
}
func (d *EchoMessageHandler) HandleDeleteMessage(idx int, msg *pglogrepl.DeleteMessage, reln *pglogrepl.RelationMessage, tableInfo *PGTableInfo) (err error) {
	log.Println("HandleDeleteMessage: ", idx, msg, reln)
	return
}
func (d *EchoMessageHandler) HandleInsertMessage(idx int, msg *pglogrepl.InsertMessage, reln *pglogrepl.RelationMessage, tableInfo *PGTableInfo) (err error) {
	log.Println("HandleInsertMessage: ", idx, msg, reln, tableInfo)
	return
}

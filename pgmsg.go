package dbsync

import (
	"errors"
	"fmt"
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
	BatchUpsert(doctype string, docs map[string]StringMap) (any, error)
	BatchDelete(doctype string, docids []string) (any, error)
}

type MessageHandler interface {
	LastCommit() int
	HandleMessage(i int, rawmsg *PGMSG) error
}

type PGMSG struct {
	LSN  string
	Xid  uint64
	Data []byte
}

type DefaultMessageHandler struct {
	lastBegin             int
	lastCommit            int
	DB                    *DBSync
	HandleBeginMessage    func(m *DefaultMessageHandler, idx int, msg *pglogrepl.BeginMessage) error
	HandleCommitMessage   func(m *DefaultMessageHandler, idx int, msg *pglogrepl.CommitMessage) error
	HandleRelationMessage func(m *DefaultMessageHandler, idx int, msg *pglogrepl.RelationMessage, tableInfo *PGTableInfo) error
	HandleUpdateMessage   func(m *DefaultMessageHandler, idx int, msg *pglogrepl.UpdateMessage, reln *pglogrepl.RelationMessage) error
	HandleDeleteMessage   func(m *DefaultMessageHandler, idx int, msg *pglogrepl.DeleteMessage, reln *pglogrepl.RelationMessage) error
	HandleInsertMessage   func(m *DefaultMessageHandler, idx int, msg *pglogrepl.InsertMessage, reln *pglogrepl.RelationMessage) error
	relnCache             map[uint32]*pglogrepl.RelationMessage
}

func (p *DefaultMessageHandler) UpdateRelation(reln *pglogrepl.RelationMessage) *PGTableInfo {
	if p.relnCache == nil {
		p.relnCache = make(map[uint32]*pglogrepl.RelationMessage)
	}
	p.relnCache[reln.RelationID] = reln

	// Query to get info on pkeys
	tableInfo, _ := p.DB.RefreshTableInfo(reln.RelationID, reln.Namespace, reln.RelationName)
	return tableInfo

	/*
			getpkeyquer := fmt.Sprintf(`SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
								FROM   pg_index i
								JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
								WHERE  i.indrelid = 'tablename'::regclass AND i.indisprimary`, reln.Namespace, reln.RelationName)
		log.Println("Query for pkey: ", getpkeyquer)
	*/
}

func (p *DefaultMessageHandler) GetRelation(relationId uint32) *pglogrepl.RelationMessage {
	if p.relnCache == nil {
		p.relnCache = make(map[uint32]*pglogrepl.RelationMessage)
	}
	reln, ok := p.relnCache[relationId]
	if !ok {
		panic("Could not find relation - Need to query DB manually")
	}
	return reln
}

func (p *DefaultMessageHandler) HandleMessage(idx int, rawmsg *PGMSG) (err error) {
	msgtype := rawmsg.Data[0]
	switch msgtype {
	case 'B':
		p.lastBegin = idx
		if p.HandleBeginMessage != nil {
			var msg pglogrepl.BeginMessage
			msg.Decode(rawmsg.Data[1:])
			return p.HandleBeginMessage(p, idx, &msg)
		} else {
			// log.Println("Begin Transaction: ", rawmsg)
		}
	case 'C':
		p.lastCommit = idx
		if p.HandleCommitMessage != nil {
			var msg pglogrepl.CommitMessage
			msg.Decode(rawmsg.Data[1:])
			return p.HandleCommitMessage(p, idx, &msg)
		} else {
			// log.Println("Commit Transaction: ", p.LastBegin, rawmsg)
		}
	case 'R':
		if p.HandleRelationMessage != nil {
			var msg pglogrepl.RelationMessage
			msg.Decode(rawmsg.Data[1:])
			// TODO - Cache this so we arent doing this again and again
			tableInfo := p.UpdateRelation(&msg)
			return p.HandleRelationMessage(p, idx, &msg, tableInfo)
		}
	case 'I':
		if p.HandleInsertMessage != nil {
			var msg pglogrepl.InsertMessage
			msg.Decode(rawmsg.Data[1:])
			reln := p.GetRelation(msg.RelationID)
			return p.HandleInsertMessage(p, idx, &msg, reln)
		}
	case 'D':
		if p.HandleDeleteMessage != nil {
			var msg pglogrepl.DeleteMessage
			msg.Decode(rawmsg.Data[1:])
			reln := p.GetRelation(msg.RelationID)
			return p.HandleDeleteMessage(p, idx, &msg, reln)
		}
	case 'U':
		if p.HandleUpdateMessage != nil {
			var msg pglogrepl.UpdateMessage
			msg.Decode(rawmsg.Data[1:])
			reln := p.GetRelation(msg.RelationID)
			return p.HandleUpdateMessage(p, idx, &msg, reln)
		}
	default:
		log.Println(fmt.Sprintf("Processing Messages (%c): ", msgtype), rawmsg)
		panic(fmt.Errorf("invalid Message Type: %c", msgtype))
	}
	return nil
}

func (p *DefaultMessageHandler) LastCommit() int {
	return p.lastCommit
}

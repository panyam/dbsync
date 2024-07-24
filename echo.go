package dbsync

import "log"

type EchoMessageHandler struct {
}

func (e *EchoMessageHandler) HandleMessage(idx int, rawmsg *PGMSG) (err error) {
	log.Println("Handling Message: ", idx, rawmsg)
	return nil
}

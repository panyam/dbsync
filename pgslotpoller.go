package dbsync

import (
	"log"
	"log/slog"
	"sync"
	"time"
)

type cmd struct {
	Name string
	Data any
}

type LogEventCallbackFunc func([]PGMSG, error) (numProcessed int, stop bool)

// The PeriodicLogReader polls the postgres replication slot at periodic intervals
// and peeks at all change capture messages and passes it to the change handler.
// Once the handler processes these messages, this reader forwards the slot's seek offset
// - marking the messages as processed.  A polling based slot consumer is ideal in a
// change/write heavy system so that the poller can control how fast it consumes/processes
// messages.
type PGSlotPoller struct {
	// How many items will be peeked at a time
	Callback LogEventCallbackFunc

	// A channel letting us know how many items are to be committed
	commitReqChan chan int

	pgrs *PGReplSlot

	isRunning                 bool
	cmdChan                   chan cmd
	wg                        sync.WaitGroup
	TimerDelayBackoffFactor   float32
	DelayBetweenPeekRetries   time.Duration
	MaxDelayBetweenEmptyPeeks time.Duration
	MaxMessagesToRead         int
}

// Creates a new slot poller for a given postgres DB.
func NewPGSlotPoller(pgrs *PGReplSlot, callback LogEventCallbackFunc) *PGSlotPoller {
	out := &PGSlotPoller{
		Callback:                  callback,
		pgrs:                      pgrs,
		TimerDelayBackoffFactor:   1.5,
		DelayBetweenPeekRetries:   100 * time.Millisecond,
		MaxDelayBetweenEmptyPeeks: 60 * time.Second,
		MaxMessagesToRead:         8192,
	}
	return out
}

func (l *PGSlotPoller) IsRunning() bool {
	return l.isRunning
}

func (l *PGSlotPoller) Stop() {
	l.cmdChan <- cmd{Name: "stop"}
	l.wg.Wait()
}

/**
 * Resume getting events.
 */
func (l *PGSlotPoller) Start() {
	l.isRunning = true
	log.Println("Slot Poller Started")
	l.wg.Add(1)
	l.commitReqChan = make(chan int)
	timerDelay := 0 * time.Second
	readTimer := time.NewTimer(0)
	defer func() {
		l.isRunning = false
		close(l.commitReqChan)
		if !readTimer.Stop() {
			<-readTimer.C
		}
		l.commitReqChan = nil
		l.wg.Done()
		log.Println("Slot Poller Stopped")
	}()
	for {
		select {
		case cmd := <-l.cmdChan:
			if cmd.Name == "stop" {
				// clean up and stop
				return
			} else if cmd.Name == "resetreadtimer" {
				readTimer.Reset(0)
			} else {
				log.Println("Invalid command: ", cmd)
			}
		case <-readTimer.C:
			msgs, err := l.pgrs.GetMessages(l.MaxMessagesToRead, false, nil)
			if err != nil {
				l.Callback(nil, err)
				return
			}
			if len(msgs) == 0 {
				// Nothing found - try again after a delay
				slog.Debug("Timer delay: ", "timerDelay", timerDelay)
				if timerDelay == 0 {
					timerDelay += l.DelayBetweenPeekRetries
				} else {
					timerDelay = (3 * timerDelay) / 2
				}
				if timerDelay > l.MaxDelayBetweenEmptyPeeks {
					timerDelay = l.MaxDelayBetweenEmptyPeeks
				}
			} else {
				timerDelay = 0

				// Here we should process the messages so we can tell the DB
				numProcessed, stop := l.Callback(msgs, err)
				err = l.pgrs.Forward(numProcessed)
				if err != nil {
					panic(err)
				}
				if stop {
					return
				}
			}
			readTimer = time.NewTimer(timerDelay)
		}
	}
}

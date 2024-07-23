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

// Type of the callback function to handle messages read (peeked) from replication slot before the offset is forwarded.
// The callback accepts the list of messages (at the head of the replication slot) and returns two outputs:
//   - numProcessed - The number of messages processed.  The poller then forwards the slot's by this amount (instead of how many it originally peeked).
//   - stop - A bool variable that signals to the poller if it should stop processing any further messages from the replication slot.
type LogEventCallbackFunc func([]PGMSG, error) (numProcessed int, stop bool)

// The PeriodicLogReader polls the postgres replication slot at periodic intervals
// and peeks at all change capture messages and passes it to the change handler.
// Once the handler processes these messages, this reader forwards the slot's seek offset
// - marking the messages as processed.  A polling based slot consumer is ideal in a
// change/write heavy system so that the poller can control how fast it consumes/processes
// messages.
type PGSlotPoller struct {
	// The callback to handle messages read (peeked) from replication slot before the offset is forwarded.
	Callback LogEventCallbackFunc

	// The poller peeks for messages in the replication slot periodically.
	// This determines delay between peeks.
	DelayBetweenPeekRetries time.Duration

	// The delay between peeks is not fixed.  Instead if no messages are found a backoff is applied
	// so this delay increases until there are more messages.  This factor determins how much the
	// delay should be increased by when no messages are found on the slot.
	TimerDelayBackoffFactor float32

	// The delay between empty peeks is capped at this amount.
	MaxDelayBetweenEmptyPeeks time.Duration

	// Maximum number of messages to read and process at a time.
	MaxMessagesToRead int

	// A channel letting us know how many items are to be committed
	commitReqChan chan int

	pgrs *ReplSlot

	isRunning bool
	cmdChan   chan cmd
	wg        sync.WaitGroup
}

// Creates a new slot poller for a given postgres DB.
func NewPGSlotPoller(pgrs *ReplSlot, callback LogEventCallbackFunc) *PGSlotPoller {
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

// Tells if the poller is running or stopped.
func (l *PGSlotPoller) IsRunning() bool {
	return l.isRunning
}

// Signals to the poller to stop consuming messages from the replication slot
func (l *PGSlotPoller) Stop() {
	l.cmdChan <- cmd{Name: "stop"}
	l.wg.Wait()
}

// Starts the event polling loop
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

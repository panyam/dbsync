package main

import (
	"fmt"

	"github.com/panyam/dbsync"
)

func main() {
	fmt.Println("vim-go")
	poller := dbsync.NewPGSlotPoller()
	poller.Start()
}

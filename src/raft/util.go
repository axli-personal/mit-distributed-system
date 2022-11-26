package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func init() {
	log.Default().SetFlags(log.Ltime | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func randomElectionTimeout() time.Duration {
	return time.Duration(200+rand.Intn(300)) * time.Millisecond
}

func stableHeartbeatTimeout() time.Duration {
	return 100 * time.Millisecond
}

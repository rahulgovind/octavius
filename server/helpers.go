package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AtomicCounter struct {
	count int
	lock  sync.Mutex
}

type Semaphore struct {
	s chan bool
}

func NewSemaphore(n int) *Semaphore {
	r := &Semaphore{make(chan bool, n)}
	// Fill semaphore
	for i := 0; i < n; i++ {
		r.s <- true
	}
	return r
}

func (sem *Semaphore) Acquire() {
	<-sem.s
}

func (sem *Semaphore) Release() {
	sem.s <- true
}

func addrToIP(addr string) string {
	ips, _ := net.LookupIP(addr)
	if len(ips) > 0 {
		return ips[0].String()
	}

	return ""
}

func AddrToHostPort(addr string) (string, int) {
	l := strings.Split(addr, ":")
	host := l[0]
	port, err := strconv.ParseInt(l[1], 10, 64)
	if err != nil {
		log.Fatal("Unable to parse port.", err)
	}
	return host, int(port)
}

// Increment adds 1 to the count and returns the old value
func (ac *AtomicCounter) Increment() int {
	ac.lock.Lock()
	defer ac.lock.Unlock()
	oldCount := ac.count
	ac.count += 1
	return oldCount
}

func Timestamp() int64 {
	return time.Now().UTC().UnixNano()
}

// InRange checks if x is in the circular range that starts from
// `start` and ends at `end` (both inclusive)
// m is the ring size
func InRange(x, start, end, m int64) bool {
	idx := (x - start + m) % m
	if idx >= 0 && idx <= (end-start+m)%m {
		return true
	}
	return false
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func calcConsistency(n int, consistency string) int {
	switch consistency {
	case "QUORUM":
		return n/2 + 1
	case "ALL":
		return n
	case "ONE":
		return 1
	default:
		return n
	}
}

func ByteCountDecimal(b int) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}
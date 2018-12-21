package main

import (
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type FailureDetector struct {
	lock     sync.Mutex // Protects seqMap
	seqMap   map[uint64]chan bool
	stopChan chan bool
	g        *Group
	seq      uint64
}

func NewFailureDetector(g *Group) *FailureDetector {
	return &FailureDetector{
		seqMap:   make(map[uint64]chan bool),
		stopChan: make(chan bool, 1),
		g:        g,
	}
}

func (fd *FailureDetector) Stop() {
	fd.stopChan <- true
}

func (fd *FailureDetector) Handler(ticker *time.Ticker, timeout time.Duration) {
	log.Debugf("Starting Failure Detector")

	g := fd.g
	for {
		select {
		case <-ticker.C:
			checklist := g.Successors(NumSuccessors)
			log.Tracef("Checking")
			for _, member := range checklist {
				log.Tracef("%v", member)
				if !fd.checkAlive(&member, timeout) {
					// We recheck RecheckCount number of times
					// to ensure that node has reallllly failed. Decreases
					// the likelihood of a false positive.

					// Is it a false positive?
					fp := false
					for i := 0; i < RecheckCount; i++ {
						if fd.checkAlive(&member, timeout) {
							fp = true
							break
						}
					}

					// Nope. Most probably not a false positive.
					// Broadcast that member has failed and mark it as failed
					if !fp {
						g.MarkFail(member)
					}
				}
			}
		case <-fd.stopChan:
			return
		}
	}
	log.Debugf("Stopping failure detector")
}

func (fd *FailureDetector) ping(member *Member) uint64 {
	fd.lock.Lock()
	defer fd.lock.Unlock()
	pingSeq := fd.seq
	fd.seq++
	fd.seqMap[pingSeq] = make(chan bool, 1)

	pingMsg := &PingMessage{
		Src: fd.g.Member().ProtoMember(),
		Seq: pingSeq,
	}

	protoPingMsg, _ := ptypes.MarshalAny(pingMsg)

	m := &Message{
		MessageType: Message_PING,
		Addr:        fd.g.Addr,
		Details:     protoPingMsg,
	}
	SendUDP(member.Addr, m)
	return pingSeq
}

// Check Alive checks if member is alive by sending a ping message
// and expecting a pong message in the given timeout
func (fd *FailureDetector) checkAlive(member *Member, timeout time.Duration) bool {
	seq := fd.ping(member)
	defer func() {
		fd.lock.Lock()
		_, ok := fd.seqMap[seq]
		if ok {
			delete(fd.seqMap, seq)
		}
		fd.lock.Unlock()
	}()

	timer := time.NewTimer(timeout)

	fd.lock.Lock()
	ch, ok := fd.seqMap[seq]
	fd.lock.Unlock()

	if !ok {
		return false
	}

	select {
	case <-timer.C:
		return false
	case <-ch:
		return true
	}
}

func (fd *FailureDetector) HandlePing(msg *Message) {
	pingMsg := new(PingMessage)
	ptypes.UnmarshalAny(msg.Details, pingMsg)

	pongMessage := &PongMessage{
		Seq: pingMsg.Seq,
	}
	protoPongMessage, _ := ptypes.MarshalAny(pongMessage)

	m := &Message{
		MessageType: Message_PONG,
		Addr:        fd.g.Addr,
		Details:     protoPongMessage,
	}
	SendUDP(pingMsg.Src.Addr, m)
}

func (fd *FailureDetector) HandlePong(msg *Message) {
	fd.lock.Lock()
	defer fd.lock.Unlock()

	pongMsg := new(PongMessage)
	ptypes.UnmarshalAny(msg.Details, pongMsg)
	log.Tracef("PONG: Received pong from %v (pingID: %v)", msg.Addr, pongMsg.Seq)
	ch, ok := fd.seqMap[pongMsg.Seq]
	if ok {
		ch <- true
	}
}

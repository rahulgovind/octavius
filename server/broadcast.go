package main

import (
	"github.com/golang-collections/go-datastructures/queue"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Broadcast interface {
	HandleBroadcast(msg *Message) // Called by net/transport interface to insert broadcast messages received
	Broadcast(msg *Message)       // Broadcast given message
	Close()                       // Stop handling broadcasts
}

// Description of Gossip broadcast
// We maintain a priority queue at each process.
// The priority queue contains the messages that need to be broadcasted
// Each time we sent out a gossip message, we decrement the TTL. If the TTL is greater than zero, we add it back to
// the priority queue if it isn't already there.
type Gossiper struct {
	lock     sync.RWMutex
	members  []Member
	pq       *queue.PriorityQueue // PriorityQueue is thread-safe
	stopChan chan bool
	sent     map[GossipId]bool
	msgCh    chan<- *Message
	g        *Group
}

type BroadcastItem struct {
	gd   *GossipDetails
	time time.Time // Time at which to broadcast message
}

func (b BroadcastItem) Compare(other queue.Item) int {
	if b.time.After(other.(BroadcastItem).time) {
		return 1
	} else if b.time.Before(other.(BroadcastItem).time) {
		return -1
	} else {
		return 0
	}
}

type GossipId struct {
	addr string
	ts   uint64
}

func NewGossiper(g *Group, msgCh chan<- *Message) *Gossiper {
	return &Gossiper{
		stopChan: make(chan bool, 1), // Allow one send to stopChan without blocking. Look at Close()
		sent:     make(map[GossipId]bool),
		msgCh:    msgCh,
		g:        g,
		pq:       queue.NewPriorityQueue(0),
	}
}

func (gs *Gossiper) HandleBroadcast(bcMsg *Message) {
	var gossipDetails GossipDetails
	err := ptypes.UnmarshalAny(bcMsg.Details, &gossipDetails)
	if err != nil {
		log.Fatal("Unable to unmarshal gossip data")
		return
	}

	msg := gossipDetails.Message
	gossipId := GossipId{
		addr: ProtoMemberToMember(*gossipDetails.Src).Addr,
		ts:   gossipDetails.Ts,
	}

	gs.lock.Lock()
	defer gs.lock.Unlock()

	if !gs.sent[gossipId] {
		gs.sent[gossipId] = true
		gs.msgCh <- msg
		log.Debug("Adding msg to channel from broadcast. ", msg)
		gs.processGossip(&gossipDetails)
	}
}

func (gs *Gossiper) Broadcast(msg *Message) {
	if gs.msgCh != nil {
		gs.msgCh <- msg
	}

	t := time.Now()
	gs.lock.Lock()
	gs.sent[GossipId{gs.g.Addr, uint64(t.UnixNano())}] = true
	gs.lock.Unlock()

	gossipData := &GossipDetails{
		Ttl:     GossipTTL,
		T:       GossipT,
		B:       GossipB,
		Ts:      uint64(t.UnixNano()),
		Message: msg,
		Src:     gs.g.Member().ProtoMember(),
	}

	gs.pq.Put(BroadcastItem{
		gossipData,
		t,
	})
}

func (gs *Gossiper) Close() {
	gs.stopChan <- true
	// In case the priority queue is empty and we might be stuck at the 'Get' part of the broadcast handler.
	// We insert a dummy message at practically infinite time to exit that part of the loop
	gs.pq.Put(BroadcastItem{time: time.Now().Add(1 * time.Hour)})
}

func (gs *Gossiper) Handler() {
	ticker := time.NewTicker(1 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			for {
				item, err := gs.pq.Get(1)
				if err != nil {
					log.Fatal("Unable to get broadcast item")
				}

				broadcastItem := item[0].(BroadcastItem)
				if broadcastItem.time.Before(time.Now()) {
					gs.processGossip(broadcastItem.gd)
				} else {
					// Insert it back. It's time hasn't come yet.
					gs.pq.Put(broadcastItem)
				}
			}
		case <-gs.stopChan:
			return
		}
	}
}

func (gs *Gossiper) processGossip(gd *GossipDetails) {
	if gd.Ttl > 0 {
		gd.Ttl--
		gs.multicast(gd)

		if gd.Ttl > 0 {
			gs.pq.Put(BroadcastItem{
				gd:   gd,
				time: time.Now().Add(time.Duration(gd.T) * time.Millisecond),
			})
		}
	}
}

// Multicast sends out the message to random members
func (gs *Gossiper) multicast(gd *GossipDetails) {
	protoGd, err := ptypes.MarshalAny(gd)
	if err != nil {
		log.Fatal("Unable to marshal gossip details")
	}

	gossipMessage := &Message{
		MessageType: Message_GOSSIP,
		Details:     protoGd,
	}

	targets := gs.g.RandomMembers(int(gd.B))
	log.Tracef("Gossip detail :: B:%v, T:%v, TTL:%v, N:%v", gd.B, gd.T, gd.Ttl)
	log.Debugf("Gossip targetss: %v", targets)

	for _, target := range targets {
		go SendUDP(target.Addr, gossipMessage)
	}

}

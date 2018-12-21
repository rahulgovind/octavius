package main

import (
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
	"os"
)

func (g *Group) MessageHandler(msgch <-chan *Message) {
	for {
		select {
		case msg := <-msgch:
			log.Tracef("%v: Received message: %v", g.Addr, msg)
			switch msg.MessageType {
			case Message_INTRO_SYN:
				g.handleMemberlistRequest(msg)
			case Message_INTRO_ACK:
				details := new(IntroducerResponse)
				ptypes.UnmarshalAny(msg.Details, details)
				g.handleMemberlistResponse(details)
			case Message_GOSSIP:
				go g.broadcaster.HandleBroadcast(msg)
			case Message_ADD:
				introMessage := new(IntroMessage)
				ptypes.UnmarshalAny(msg.Details, introMessage)
				g.handleAddNode(introMessage)
			case Message_PING:
				g.handlePing(msg)
			case Message_PONG:
				g.handlePong(msg)
			case Message_SUSPECT:
				g.handleSuspect(msg)
			}
		case <-g.handlerStopChan:
			log.Printf("Stopping message handler")
			return
		}
	}
}

func (g *Group) stopMessageHandler() {
	g.handlerStopChan <- true
}

func (g *Group) handleMemberlistRequest(message *Message) {
	joinMsg := new(JoinMessage)
	ptypes.UnmarshalAny(message.Details, joinMsg)

	newMember := ProtoMemberToMember(*joinMsg.Member)
	if !g.inCoordinators(newMember.Addr) {
		g.AddMember(newMember)
	}

	introResp := &IntroducerResponse{
		Members: g.ml.ProtoMembers(),
	}
	log.Debugf("Sending members: %v ", introResp.Members)

	details, err := ptypes.MarshalAny(introResp)
	if err != nil {
		log.Fatal("Unable to marshal membership list")
	}

	m := &Message{
		Addr:        g.Addr,
		MessageType: Message_INTRO_ACK,
		Details:     details,
	}
	SendTCP(newMember.Addr, m)
}

func (g *Group) handleMemberlistResponse(response *IntroducerResponse) {
	log.Debugf("Received list: %v", response)
	g.HandleJoin(ProtoMembersToMembers(response.Members))
}

func (g *Group) handleAddNode(introMsg *IntroMessage) {
	g.AddMember(ProtoMemberToMember(*introMsg.Member))
}

func (g *Group) handlePing(msg *Message) {
	log.Tracef("PING: Received ping from %v", msg.Addr)
	g.detector.HandlePing(msg)
}

func (g *Group) handlePong(msg *Message) {
	g.detector.HandlePong(msg)
}

func (g *Group) handleSuspect(msg *Message) {
	suspectMsg := new(SuspectMessage)
	ptypes.UnmarshalAny(msg.Details, suspectMsg)
	suspect := ProtoMemberToMember(*suspectMsg.Suspect)
	if suspect.Addr == g.Addr {
		if !g.leaving {
			log.Debugf("I have been incorrectly suspected. Committing sui cide. :(")
			os.Exit(0)
		}
	} else {
		go g.RemoveMember(suspect)
	}
}

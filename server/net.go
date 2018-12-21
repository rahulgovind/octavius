package main

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
)

const (
	UDPBufferSize int = 65536
)

func MarshalMessage(m *Message) []byte {
	data, err := proto.Marshal(m)
	if err != nil {
		log.Fatal("Unable to marshal given data.", *m)
	}
	return data
}

func SendUDP(to string, m *Message) error {
	data := MarshalMessage(m)

	sock, err := net.Dial("udp", to)
	if err != nil {
		log.Debugf("Unable to establish UDP connection to %v", to)
		return err
	}
	defer sock.Close()

	_, err = sock.Write(data)
	log.Tracef("Packet size: %v", len(data))
	if err != nil {
		log.Debugf("Unable to send message to node: %v", err)
	}

	return nil
}

func SendTCP(to string, m *Message) error {
	data := MarshalMessage(m)
	conn, err := net.Dial("tcp", to)
	if err != nil {
		log.Debugf("Unable to establish TCP Client to %v", to)
		return err
	}
	defer conn.Close()

	_, err = conn.Write(data)
	if err != nil {
		log.Debugf("Unable to send message to node: %v", err)
	}

	return nil
}

func MulticastUDP(to []string, m *Message) error {
	for _, t := range to {
		err := SendUDP(t, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func ListenUDP(addr string, msgch chan<- *Message) {
	log.Debugf("Attempting to listen on %v", addr)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalf("Unable to resolve address: %v", addr)
	}

	listener, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("Unable to listen on %v. Error: %v", addr, err)
	}

	log.Debug("Waiting for incoming UDP connections")
	for {
		buf := make([]byte, UDPBufferSize)
		n, addr, err := listener.ReadFrom(buf)

		if err != nil {
			log.Debug(err)
			return
		}

		m := new(Message)
		proto.Unmarshal(buf[:n], m)
		log.Tracef("Received incoming connection: %v", addr)
		msgch <- m
	}
}

func ListenTCP(addr string, msgch chan<- *Message) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Unable to listen for TCP on %v", addr)
	}
	defer l.Close()

	log.Debug("Waiting for incoming TCP Connections")
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Debugf("Unable to process connection: %v", err)
			continue
		}

		go handleConnection(conn, msgch)
	}
}

func handleConnection(conn net.Conn, msgch chan<- *Message) {
	defer conn.Close()
	log.Debugf("Received incoming connection: %v", conn.RemoteAddr())

	var buf bytes.Buffer
	//buf := make([]byte, UDPBufferSize)
	_, err := io.Copy(&buf, conn)

	if err != nil {
		log.Debug(err)
		return
	}

	m := new(Message)
	proto.Unmarshal(buf.Bytes(), m)
	msgch <- m
}

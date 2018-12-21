package main

import (
	"bufio"
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"time"
)

func (g *Group) LaunchSockStream(filename string, port int, timeout time.Duration) {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
	if err != nil {
		log.Errorf("Unable to listen on socket: %v", err)
		return
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Error("Unable to accept connection")
			continue
		}

		go g.HandleSockStream(conn, filename, timeout, 10000)
	}
}

type LineResult struct {
	line string
	err  error
}

func (g *Group) HandleSockStream(conn net.Conn, filename string, timeout time.Duration, maxTuples int) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	scanChan := make(chan LineResult)
	go func(scanChan chan<- LineResult, scanner *bufio.Scanner) {
		for scanner.Scan() {
			line := scanner.Text()
			scanChan <- LineResult{line, nil}
		}
		scanChan <- LineResult{"", io.EOF}
	}(scanChan, scanner)

	outputBuffer := bytes.NewBuffer(nil)
	counter := 0
	for {
		select {
		case <-ticker.C:
			if counter > 0 {
				log.Debug("Timeout. Uploading file.")
				g.fileChan <- CreateTempFile(bytes.NewBuffer(outputBuffer.Bytes()), filename, -1, JsonFile)
				outputBuffer.Reset()
				counter = 0
			}
		case ls := <-scanChan:
			if ls.err != nil {
				if ls.err == io.EOF && counter > 0 {
					g.fileChan <- CreateTempFile(bytes.NewBuffer(outputBuffer.Bytes()), filename, -1, JsonFile)
					outputBuffer.Reset()
					return
				}
				log.Debug("Stopping connection: ", ls.err)
				return
			}

			_, err := fmt.Fprintln(outputBuffer, ls.line)
			if err != nil {
				log.Fatal("Buffer write error: ", err)
			}
			counter += 1
			if counter >= maxTuples && maxTuples != -1 {
				g.fileChan <- CreateTempFile(bytes.NewBuffer(outputBuffer.Bytes()), filename, -1, JsonFile)
				outputBuffer.Reset()
				counter = 0
			}
		}
	}
}

package main

import (
	"errors"
	"fmt"
	"github.com/rahulgovind/octavius"
	log "github.com/sirupsen/logrus"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type GetLatestRPCReply struct {
	Files        []File
	HttpAddr     string
	FirstVersion int
	Success      bool // Success = false means the connection RPC failed or remote machine does not have file
}

type GetLatestRPCArgs struct {
	NumVersions  int
	SdfsFilename string
}

type ReceiveFileRPCArgs struct {
	File File
	Addr string
}

type Client struct {
	client      *rpc.Client
	refreshTime time.Time
	sem         *Semaphore
	addr        string
	lock        sync.Mutex
}

func NewClient(addr string) *Client {
	return &Client{
		client:      nil,
		refreshTime: time.Now(),
		sem:         NewSemaphore(1),
		addr:        addr,
	}
}

func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	c.sem.Acquire()
	defer c.sem.Release()

	var err error
	c.lock.Lock()
	if c.client == nil {
		c.client, err = rpc.Dial("tcp", c.addr)
		if err != nil {
			c.client = nil
			log.Errorf("Unable to connect to RPC Server %v: %v", c.addr, err)
			c.lock.Unlock()
			return err
		}
		c.refreshTime = time.Now()
	}
	c.lock.Unlock()
	err = c.client.Call(serviceMethod, args, reply)
	if err != nil {
		log.Errorf("Unable to make call to RPC Server %v: %v", c.addr, err)
		c.client = nil
	}
	return err
}

func (c *Client) Close() {
	// Doesn't do anything for now
}

type ClientManager struct {
	lock      sync.Mutex
	clientMap map[string]*Client
}

func (cm *ClientManager) Dial(addr string) *Client {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	if _, ok := cm.clientMap[addr]; !ok {
		cm.clientMap[addr] = NewClient(addr)
	}

	return cm.clientMap[addr]
}

var defaultClientManager *ClientManager

func init() {
	defaultClientManager = &ClientManager{clientMap: make(map[string]*Client)}
}

func RPCDial(addr string) *Client {
	return defaultClientManager.Dial(addr)
}

// Upload File copies a given File to the remote address.
// It is not asynchronous
// Upload File returns true if the remote address receives the File
// and also processes the request for the File (Commit/Abort)
func (g *Group) UploadFile(f File, member Member) (bool, error) {
	client := RPCDial(member.RPCAddr)
	defer client.Close()

	var reply bool
	args := ReceiveFileRPCArgs{
		File: f,
		Addr: octavius.GetHTTPAddr(),
	}

	log.Debugf("Uploading File to %v", member.RPCAddr)
	start := time.Now()
	err := client.Call("Command.ReceiveFileRPC", args, &reply)
	elapsed := time.Since(start)

	log.Infof("Calling ReceiveFileRPC took %v", elapsed)
	if err != nil {
		log.Error("RPC Call failed: ", err)
		return false, err
	}
	return reply, nil
}

func (g *Group) SendFile(f File, consistency string) bool {
	replicas := g.FindReplicas(f.File)

	sentChan := make(chan bool, ReplicationFactor)
	successCount := 0
	totalCount := 0

	g.NumMembers()
	W := calcConsistency(len(replicas), consistency)
	W = Max(1, W) // Can't do with just 1 replica in any case

	for _, replica := range replicas {
		go func(replica Member) {
			b, err := g.UploadFile(f, replica)
			if err != nil {
				log.Error("Upload failed: ", err)
			}
			sentChan <- b
		}(replica)
	}

	for totalCount < len(replicas) && successCount < W {
		success := <-sentChan
		totalCount += 1
		if success {
			successCount += 1
		}
	}

	log.Info("SendFile: %v, %v, %v", successCount, totalCount, W)
	if successCount >= W {
		return true
	} else {
		return false
	}
}

func (g *Group) DeleteFunc(sdfsFile string) {
	f := CreateTombstone(sdfsFile)
	g.SendFile(f, "ALL")
	fmt.Println("Finishing sending delete commands")
}

func (g *Group) SendFunc(localFile string, sdfsFile string, consistency string) bool {
	start := time.Now()
	absLocalFile, _ := filepath.Abs(localFile)
	f := CreateLocalFile(absLocalFile, sdfsFile, BinaryFile)
	result := g.SendFile(f, consistency)
	f.Delete()
	fmt.Println("Finished uploading files")
	elapsed := time.Since(start)

	fmt.Printf("<put> took %s\n", elapsed)
	return result
}

func (g *Group) SendJson(localFile string, sdfsFile string) bool {
	start := time.Now()
	absLocalFile, _ := filepath.Abs(localFile)
	f := CreateLocalFile(absLocalFile, sdfsFile, JsonFile)
	result := g.SendFile(f, "ALL")
	fmt.Println("Finished uploading files")
	elapsed := time.Since(start)

	fmt.Printf("<put-json> took %s\n", elapsed)
	return result
}

func (g *Group) CheckExists(f File, member Member) (bool, error) {
	log.Debug("Calling CheckExists RPC")
	client := RPCDial(member.RPCAddr)
	defer client.Close()

	var reply bool
	err := client.Call("Command.CheckExists", f, &reply)
	if err != nil {
		return false, err
	}
	return reply, nil
}

func (g *Group) GetLatest(sdfsFile string, member Member, numVersions int) (GetLatestRPCReply, error) {
	log.Debug("Calling GetLatestFiles RPC")
	client := RPCDial(member.RPCAddr)
	defer client.Close()

	var reply GetLatestRPCReply
	args := GetLatestRPCArgs{NumVersions: numVersions, SdfsFilename: sdfsFile}
	err := client.Call("Command.GetLatestFiles", args, &reply)
	if err != nil {
		return GetLatestRPCReply{Success: false}, err
	}
	return reply, nil
}

func (g *Group) GetLatestFileReader(sdfsFile string) (reader io.Reader, fileMissing bool, err error) {
	replicas := g.FindReplicas(sdfsFile)
	if len(replicas) == 0 {
		err = errors.New("no replicas")
		return
	}

	getChan := make(chan GetLatestRPCReply, ReplicationFactor)
	for _, replica := range replicas {
		go func(replica Member) {
			r, err := g.GetLatest(sdfsFile, replica, 0)
			if err != nil {
				log.Debug(err)
			}
			getChan <- r
		}(replica)
	}

	successCount := 0
	totalCount := 0
	var maxSequence int64 = -1
	var bestReply GetLatestRPCReply

	for totalCount < len(replicas) && successCount < 1 {
		totalCount += 1
		r := <-getChan
		if r.Success {
			successCount += 1
			if len(r.Files) > 0 && r.Files[len(r.Files)-1].Sequence > maxSequence {
				maxSequence = r.Files[len(r.Files)-1].Sequence
				bestReply = r
			}
		} else {
			log.Debug("Download failed")
		}

	}

	numFiles := len(bestReply.Files)
	if maxSequence < 0 || numFiles == 0 {
		fileMissing = true
		return
	} else {
		file := bestReply.Files[len(bestReply.Files)-1]
		reader, err = octavius.DownloadReader(bestReply.HttpAddr, file.Link)
		return
	}
}

// GetFiles gets `numVersions` versions of the given file.
// If numVersions = 0, it gets only the latest file
func (g *Group) GetFiles(localFile, sdfsFile, consistency string, numVersions int) bool {
	onlyOne := numVersions == 0
	localFile, _ = filepath.Abs(localFile)

	replicaList := g.FindReplicas(sdfsFile)
	getChan := make(chan GetLatestRPCReply, ReplicationFactor)

	successCount := 0
	totalCount := 0

	var maxSequence int64 = -1
	var bestReply GetLatestRPCReply

	R := calcConsistency(len(replicaList), "ALL")

	log.Debugf("Consistency level: %v. R = %v", consistency, R)
	log.Debugf("Getting File %v", localFile)

	for _, replica := range replicaList {
		go func(replica Member) {
			r, err := g.GetLatest(sdfsFile, replica, numVersions)
			if err != nil {
				log.Debug(err)
			}
			getChan <- r
		}(replica)
	}

	for totalCount < len(replicaList) && successCount < R {
		totalCount += 1
		r := <-getChan
		if r.Success {
			successCount += 1
			if len(r.Files) > 0 && r.Files[len(r.Files)-1].Sequence > maxSequence {
				maxSequence = r.Files[len(r.Files)-1].Sequence
				bestReply = r
			}
		} else {
			log.Debug("Download failed")
		}
	}

	numFiles := len(bestReply.Files)

	if maxSequence < 0 || numFiles == 0 {
		fmt.Println("File not available")
		return false
	} else {
		if numFiles < numVersions {
			fmt.Printf("Only have %v versions (<%v). Proceeding to download all.\n",
				len(bestReply.Files), numVersions)
		}

		log.Debugf("The max File id is: %v", maxSequence)
		log.Debug("Best Reply: ", bestReply)

		var m int
		if numVersions == 0 {
			m = Min(1, len(bestReply.Files))
		} else if numVersions < 0 {
			m = len(bestReply.Files)
		} else {
			m = Min(numVersions, len(bestReply.Files))
		}
		for i, file := range bestReply.Files[len(bestReply.Files)-m:] {
			localFilename := ""

			if onlyOne {
				localFilename = localFile
			} else {
				localFilename = fmt.Sprintf("%v-%v", localFile, i+(len(bestReply.Files)-m)+1)
			}

			localFile, err := os.Create(localFilename)
			if err != nil {
				log.Error("Unable to create local file: ", err)
				fmt.Println("Unable to complete file downloads")
				return false
			}

			err = octavius.Download(bestReply.HttpAddr, file.Link, localFile)

			localFile.Close()
			if err != nil {
				log.Error(err)
				fmt.Println("Unable to complete file downloads")
				return false
			}
		}
		fmt.Println("Completed file downloads")
	}
	return true
}

func (g *Group) GetFunc(localFile, sdfsFile, consistency string) {
	start := time.Now()
	g.GetFiles(localFile, sdfsFile, consistency, 0)
	elapsed := time.Since(start)
	fmt.Printf("<get> took %s\n", elapsed)
}

func (g *Group) GetNFunc(localFile, sdfsFile, consistency string, numVersions int) {
	start := time.Now()
	g.GetFiles(localFile, sdfsFile, consistency, numVersions)
	elapsed := time.Since(start)
	fmt.Printf("<get-versions> took %s\n", elapsed)
}

func (g *Group) GetStatus(filename string) (octavius.StatusResponse, error) {
	replicas := g.FindReplicas(filename)
	if len(replicas) == 0 {
		return octavius.StatusResponse{}, errors.New("job not found")
	}

	log.Info("Founding replicas for status. Dialing %v", replicas[0].RPCAddr)
	client := RPCDial(replicas[0].RPCAddr)
	defer client.Close()

	var reply octavius.StatusResponse
	args := octavius.StatusRequestArgs{
		Filename: filename,
	}

	log.Infof("Requesting status from primary replica", replicas[0].Addr)
	err := client.Call("Command.GetFileStatus", args, &reply)

	if err != nil {
		log.Error(err)
	}
	return reply, err
}

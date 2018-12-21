package main

import (
	"errors"
	"fmt"
	"github.com/rahulgovind/octavius"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"time"
)

type Command struct {
	Group *Group
}

func (g *Group) RPCServerListener() {
	command := &Command{
		Group: g,
	}
	rpc.Register(command)
	tcpAddr, err := net.ResolveTCPAddr("tcp", g.RPCAddr)
	if err != nil {
		log.Fatal("Fail to resolve in RPC", err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	log.Print(tcpAddr)
	if err != nil {
		log.Fatal(err)
	}

	log.Debugf("Waiting for RPCs on %v", tcpAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		log.Debug("Incoming RPC Request")
		go rpc.ServeConn(conn)
	}
}

// Receive File RPC does all the magic
func (c *Command) ReceiveFileRPC(args *ReceiveFileRPCArgs, reply *bool) error {
	log.Debug("ReceiveFileRPC called")
	g := c.Group

	// Already have the file. Not going to download it again!
	if g.fr.Contains(args.File) {
		*reply = true
		return nil
	}

	fCount := g.counter.Increment()
	srcFilename := args.File.toFilename(fCount)
	srcFile := args.File
	srcFile.Link = string(srcFilename)

	srcFileHandle, _ := octavius.Create(string(srcFilename))
	start := time.Now()
	err := octavius.Download(args.Addr, args.File.Link, srcFileHandle)
	elapsed := time.Since(start)
	log.Infof("Download took %v", elapsed)

	if err != nil {
		log.Error("Receive file RPC Failed: ", err)
		*reply = false
		srcFileHandle.Close()
		srcFile.Delete()
		return err
	}
	g.AddFile(srcFile)
	*reply = true
	return nil
}

func (c *Command) CheckExists(f File, reply *bool) error {
	log.Debug("CheckExists RPC Called")
	g := c.Group
	*reply = g.fr.Contains(f)

	log.Printf("CheckExists reply: %v", *reply)
	return nil
}

func (c *Command) GetLatestFiles(args GetLatestRPCArgs, reply *GetLatestRPCReply) error {
	log.Debug("GetLatestFiles RPC called")
	files := c.Group.fr.Find(args.SdfsFilename)
	DeleteAll(files)
	if len(files) > 0 && files[0].Tombstone {
		files = files[1:]
	}

	m := len(files)
	if args.NumVersions == 0 {
		m = Min(1, len(files))
	} else if args.NumVersions > 0 {
		m = Min(args.NumVersions, len(files))
	}

	*reply = GetLatestRPCReply{
		Files:        files,
		HttpAddr:     octavius.GetHTTPAddr(),
		FirstVersion: len(files) - m + 1, // Ignore this
		Success:      true,
	}
	return nil
}

func (c *Command) SubmitJob(args octavius.SubmitJobArgs, reply *octavius.SubmitJobResponse) error {
	t, err := c.Group.SubmitJob(args.Topology, args.Program)
	if err != nil {
		return err
	}
	*reply = octavius.SubmitJobResponse{Topology: t}
	return nil
}

func (c *Command) GetFileStatus(args octavius.StatusRequestArgs, r *octavius.StatusResponse) error {
	g := c.Group
	files := g.fr.Find(args.Filename)
	var result []octavius.Status
	for _, file := range files {
		if file.IsJson() {
			status := octavius.INPROGRESS
			if file.AutoTombstone {
				status = octavius.COMPLETED
			}

			result = append(result, octavius.Status{
				Block:   file.Sequence,
				Status:  status,
				Updated: file.Updated,
			})
		}
		file.Delete()
	}
	*r = octavius.StatusResponse{Name: args.Filename, Resp: result}
	if len(files) == 0 {
		return errors.New(fmt.Sprintf("no job %v found", args.Filename))
	}
	return nil
}

func (c *Command) GetTopologyStatus(topology octavius.Topology, r *[]octavius.StatusSummary) error {
	g := c.Group
	var result []octavius.StatusSummary
	log.Info("Received topology status request")
	for _, node := range topology {
		resp, err := g.GetStatus(node.Name)
		log.Info(resp)
		if err != nil {
			log.Info("Received msg: ", err)
			result = append(result, octavius.StatusSummary{node.Name, -1, -1, time.Time{}})
		} else {
			r := resp.Summarize()
			if node.Sink {
				r.Completed = r.Total
			}
			result = append(result, r)

		}
	}
	log.Info("Result: ", result)
	*r = result
	return nil
}

func (c *Command) CountTuples(filename string, r *int) error {
	g := c.Group
	replicas := g.FindReplicas(filename)
	if len(replicas) == 0 {
		*r = -1
		return errors.New("file not found")
	}

	if !g.isCoordinator {
		*r = g.CountTuples(filename)
		return nil
	} else {
		log.Debugf("Found replicas for status. Dialing %v", replicas[0].RPCAddr)
		client := RPCDial(replicas[0].RPCAddr)
		defer client.Close()
		var reply int
		err := client.Call("Command.CountTuples", filename, &reply)
		if err != nil {
			return err
		}
		*r = reply
	}
	return nil
}

package octavius

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"reflect"
	"time"
)

const (
	INPROGRESS = 1
	COMPLETED  = 2
)

type Tuple []interface{}

type Topology []Node

type Node struct {
	Name         string
	Output       []string
	BlockSize    int
	InputFormat  string
	OutputFormat string
	Sink         bool
	P            Program
	f            reflect.Value
	input        reflect.Value
	output       reflect.Value
}

type SubmitJobArgs struct {
	Topology Topology
	Program  Program
}

type SubmitJobResponse struct {
	Topology Topology
}

type StatusRequestArgs struct {
	Filename string
}

type StatusSummary struct {
	Name      string
	Completed int
	Total     int
	Updated   time.Time
}

type Status struct {
	Block   int64
	Status  int
	Updated time.Time
}

type StatusResponse struct {
	Name string
	Resp []Status
}

type UploadRequest struct {
	Topology Topology
	Program  Program
}

func (t Topology) Same(t2 Topology) bool {
	if len(t) != len(t2) {
		return false
	}
	n1 := make(map[string]bool)
	for _, x := range t {
		n1[x.Name] = true
	}
	for _, x := range t2 {
		if _, ok := n1[x.Name]; !ok {
			return false
		}
	}
	return true
}

func (r *StatusResponse) Summarize() (summary StatusSummary) {

	var progress, completed int
	maxTime := time.Time{}
	for _, status := range r.Resp {
		if status.Status == INPROGRESS {
			progress += 1
		} else if status.Status == COMPLETED {
			completed += 1
		}
		if maxTime.Before(status.Updated) {
			maxTime = status.Updated
		}
	}
	summary.Name = r.Name
	summary.Completed = completed
	summary.Total = progress + completed
	summary.Updated = maxTime
	return
}

func (n *Node) Load() {
	if !n.Sink {
		log.Debug("Loading new Bolt: %v", n.Name)
		symbols := n.P.Load(n.Name, n.Name+"Input", n.Name+"Output")
		n.input = reflect.ValueOf(symbols[1].(func() interface{})())
		n.f = reflect.ValueOf(symbols[0])
		n.output = reflect.ValueOf(symbols[2].(func() interface{})())
	} else {
		log.Debug("Loading new Sink: %v", n.Name)
		symbols := n.P.Load(n.Name + "Input")
		n.input = reflect.ValueOf(symbols[0].(func() interface{})())
	}
}

func (n *Node) GetInput() reflect.Value {
	return n.input
}

func (n *Node) GetOutput() reflect.Value {
	return n.output
}

func (n *Node) GetFunction() reflect.Value {
	return n.f
}

func (t *Topology) getNodeNames() (result []string) {
	for _, n := range *t {
		result = append(result, n.Name)
	}
	return
}

func CheckConflict(t1 Topology, t2 Topology) bool {
	for _, n1 := range t1 {
		for _, n2 := range t2 {
			if n1.Name == n2.Name {
				return true
			}
		}
	}
	return false
}
func CreateNodeConfiguration(node Node) io.Reader {
	// Program not initialized
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(node)
	if err != nil {
		log.Fatal("Unable to create node configuration: ", err)
	}

	dec := gob.NewDecoder(bytes.NewReader(buf.Bytes()))
	n := new(Node)
	dec.Decode(n)

	return bytes.NewReader(buf.Bytes())
}

type Crane struct {
	topology    Topology
	coordinator string
	program     Program
	active      bool
}

func NewCrane(coordinator string, topology Topology, program Program) *Crane {
	return &Crane{
		topology:    topology,
		coordinator: coordinator,
		program:     program,
	}
}

func (c *Crane) UploadProgram() error {
	client := RPCDial(c.coordinator)
	args := UploadRequest{Topology: c.topology, Program: c.program}
	var r SubmitJobResponse
	err := client.Call("Command.SubmitJob", args, &r)
	if err != nil {
		return errors.New(fmt.Sprintf("upload failed: %v", err))
	}
	c.topology = r.Topology
	c.active = true
	return nil
}

func GetTopologyStatus(coordinator string, topology Topology) ([]StatusSummary, error) {
	fmt.Printf("Dialing %v\n", coordinator)
	client := RPCDial(coordinator)
	defer client.Close()

	var reply []StatusSummary
	log.Info("Making summary call")
	err := client.Call("Command.GetTopologyStatus", topology, &reply)
	return reply, err
}

func (c *Crane) GetTopologyStatus() ([]StatusSummary, error) {
	if !c.active {
		return nil, errors.New("crane has not been uploaded yet")
	}
	return GetTopologyStatus(c.coordinator, c.topology)
}

func (c *Crane) PrintStatus() {
	resp, _ := c.GetTopologyStatus()
	for _, summary := range resp {
		fmt.Printf("%v: Completed %v/%v.\t%v Left\n", summary.Name, summary.Completed, summary.Total,
			summary.Total-summary.Completed)
	}
}

func Count(coordinator string, filename string) (int, error) {
	fmt.Printf("Dialing %v\n", coordinator)
	client := RPCDial(coordinator)
	defer client.Close()

	var reply int
	log.Info("Making summary call")
	err := client.Call("Command.CountTuples", filename, &reply)
	return reply, err
}

func (c *Crane) Count(filename string) (int, error) {
	return Count(c.coordinator, filename)
}

func (c *Crane) MonitorStatus() {
	for {
		c.PrintStatus()
		<-time.NewTimer(time.Second).C
	}
}

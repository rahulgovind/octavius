package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang-collections/go-datastructures/queue"
	"github.com/rahulgovind/octavius"
	log "github.com/sirupsen/logrus"
	"io"
	"reflect"
	"sync"
	"time"
)

// We "install" a program by installing its topology
// The topology is installed by converting each node in the program to a "program file" which is simply
// some marshalled data that contains

type ProgramRegistry struct {
	hookMap map[string]octavius.Node
	lk      sync.Mutex
	g       *Group
}

var executionChan chan ExecuteContext
var executionQueue *queue.Queue

type ExecuteContext struct {
	n *octavius.Node
	f File
}

func NewProgramRegistry() *ProgramRegistry {
	return &ProgramRegistry{
		hookMap: make(map[string]octavius.Node),
	}
}

func init() {
	gob.Register(octavius.Tuple{})
	gob.Register(octavius.Node{})
	gob.Register(octavius.Program{})
	executionQueue = queue.New(1024)
	executionChan = make(chan ExecuteContext, 10240)
}

func (g *Group) UploadTopology(topology octavius.Topology, defaultProgram octavius.Program) (octavius.Topology, error) {
	// First check if program is valid
	topology2 := make([]octavius.Node, 0)
	for _, node := range topology {
		log.Info(node.P.Source)
		if node.P.Source == "" {
			node.P = defaultProgram
		}
		topology2 = append(topology2, node)
	}

	for _, node := range topology2 {
		f := CreateTempFile(octavius.CreateNodeConfiguration(node), node.Name, -1, ProgramFile)
		log.Infof("Uploading program file %v", node.Name)
		success := g.SendFile(f, "ALL")
		if success == false {
			log.Error("Program upload failed")
			return topology, errors.New("program upload failed")
		}
	}

	return topology, nil
}

func (pr *ProgramRegistry) registerProgram(f File) (*octavius.Node, error) {
	fs, err := octavius.Open(f.Link)
	if err != nil {
		log.Fatalf("Unable to open file %v", f.Link)
	}
	defer fs.Close()

	dec := gob.NewDecoder(fs)

	n := new(octavius.Node)
	err = dec.Decode(&n)
	if err != nil {
		log.Fatal("Unable to decode node configuration: ", err)
	}

	log.Info("INSTALL: Registering Program %v", f)
	n.Load()
	pr.hookMap[n.Name] = *n
	return n, nil
}

func Executor(fileChan chan<- File) {
	for {
		ctx := <-executionChan
		execute(ctx.n, ctx.f, fileChan)
	}
}

func StartExecutors(count int, fileChan chan<- File) {
	for i := 0; i < count; i++ {
		go Executor(fileChan)
	}
}

type Decoder interface {
	Decode(interface{}) error
}

type Encoder interface {
	Encode(interface{}) error
}

type LineDecoder struct {
	rd *bufio.Reader
}

func NewLineDecoder(file io.Reader) *LineDecoder {
	return &LineDecoder{bufio.NewReader(file)}
}

func (ld *LineDecoder) Decode(v interface{}) error {
	line, err := ld.rd.ReadString('\n')

	if err != nil {
		return err
	}

	line = line[:len(line)-1]
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(line))

	return nil
}

type LineEncoder struct {
	w io.Writer
}

func NewLineEncoder(w io.Writer) *LineEncoder {
	return &LineEncoder{w}
}

func (ld *LineEncoder) Encode(v interface{}) error {
	_, err := fmt.Fprintln(ld.w, v)

	if err != nil {
		return err
	}

	return nil
}

func getEncoder(format string, w io.Writer) Encoder {
	var encoder Encoder
	switch format {
	case "gob":
		encoder = gob.NewEncoder(w)
	case "json":
		encoder = json.NewEncoder(w)
	case "line":
		encoder = NewLineEncoder(w)
	}
	return encoder
}

func getDecoder(format string, r io.Reader) Decoder {
	var decoder Decoder
	switch format {
	case "line":
		decoder = NewLineDecoder(r)
	case "json":
		decoder = json.NewDecoder(r)
	case "gob":
		decoder = gob.NewDecoder(r)
	}
	return decoder
}

func execute(n *octavius.Node, f File, fileChan chan<- File) {
	function := n.GetFunction()
	inputFile, err := octavius.Open(f.Link)
	if err != nil {
		log.Fatalf("Unable to open inputFile %v", f.Link)
	}
	defer inputFile.Close()

	log.Infof("Running %v:\t: Seq: %v\n", n.Name, f.Sequence)

	inp := reflect.New(n.GetInput().Type())
	seq := f.Sequence
	decoder := getDecoder(n.InputFormat, inputFile)
	outputBuffer := bytes.NewBuffer(nil)
	encoder := getEncoder(n.OutputFormat, outputBuffer)
	counter := 0

	buffered := false
	if n.BlockSize != 0 {
		buffered = true
	}

	start := time.Now()
	for {
		err := decoder.Decode(inp.Interface())
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Infof("File Size: ", octavius.FileSize(f.Link))
			log.Fatalf("Decoder error: %v", err)
		}

		// Actually run the function
		result := function.Call([]reflect.Value{inp.Elem()})[0]
		if counter <= 2 {
			log.Infof("INPUT: %v\tOUTPUT: %v", inp.Elem(), result)
		}
		counter += 1

		for i := 0; i < result.Len(); i++ {
			err := encoder.Encode(result.Index(i).Interface())
			if err != nil {
				log.Fatal("Unable to encode output: ", err)
			}
		}

		if buffered && counter == n.BlockSize && (seq%1000 < 999) {
			for _, outputFilename := range n.Output {
				outputFile := CreateTempFile(bytes.NewReader(outputBuffer.Bytes()), outputFilename, seq, JsonFile)
				seq += 1
				fileChan <- outputFile
			}
			// Clear Output array
			outputBuffer.Reset()
			encoder = getEncoder(n.OutputFormat, outputBuffer)
			counter = 0
		}
	}

	for _, outputFilename := range n.Output {
		reader := bytes.NewReader(outputBuffer.Bytes())
		outputFile := CreateTempFile(reader, outputFilename, seq, JsonFile)
		fileChan <- outputFile
	}
	log.Debug("Creating autotombstone on ", f)
	fileChan <- CreateAutoTombstone(f)
	f.Delete()
	elapsed := time.Since(start)
	log.Infof("Running %v took %v", n.Name, elapsed)
}

// ProcessFile processes a json/program file
// Given a Json file, process file checks if a corresponding program is installed
// and runs the program using this file
// Given a Program file, it installs the program and then runs the program for all corresponding json files
// present
func (pr *ProgramRegistry) ProcessFile(f File, fr *FileRing) {
	pr.lk.Lock()
	defer pr.lk.Unlock()

	if f.IsJson() && !f.Deleted() {
		if n, ok := pr.hookMap[f.File]; ok {
			if !n.Sink {
				executionChan <- ExecuteContext{&n, f.Copy()}
			}
		}
	} else if f.IsProgram() && !f.Deleted() {
		if _, ok := pr.hookMap[f.File]; !ok {
			n, err := pr.registerProgram(f)
			if err != nil {
				log.Fatal("Unable to register program")
			}

			if !n.Sink {
				files := fr.Search(f.Id(), f.Id())
				for _, file := range files {
					if file.IsJson() && !file.Deleted() {
						log.Info("Queue size: ", executionQueue.Len())
						if file.File == "Sink" {
							log.Fatal("Encountered sink with autotombstone in ProcessFile")
						}
						executionChan <- ExecuteContext{n, file}
						if err != nil {
							log.Fatal("Failed to add to execution queue: ", err)
						}
					}
				}
			}
		}
	}
	f.Delete()
}

func (pr *ProgramRegistry) PrintRegisteredPrograms() {
	pr.lk.Lock()
	defer pr.lk.Unlock()
	for k := range pr.hookMap {
		fmt.Println(k)
	}
}

func (pr *ProgramRegistry) Count(program string, files []File) int {
	pr.lk.Lock()
	n, ok := pr.hookMap[program]
	pr.lk.Unlock()
	if !ok {
		return -1
	}

	start := time.Now()

	count := 0
	for _, file := range files {
		if !file.IsJson() || file.Deleted() {
			continue
		}
		f, err := octavius.Open(file.Link)

		if err != nil {
			log.Error("Unable to open file: ", err)
			return -1
		}

		inp := reflect.New(n.GetInput().Type())
		decoder := getDecoder(n.InputFormat, f)
		for {
			err := decoder.Decode(inp.Interface())
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Info("File Size: ", octavius.FileSize(file.Link))
				log.Fatalf("Decoder error: %v", err)
			}
			count += 1
		}
		file.Delete()
	}
	elapsed := time.Since(start)
	log.Infof("Counting took %v", elapsed)
	return count
}

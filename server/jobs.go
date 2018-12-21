package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/rahulgovind/octavius"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"net/http"
	"time"
)

/*
The job tracker is like a very lazy and stupid manager.

It maintains all jobs in a file called --config

How the job tracker submits jobs:
Adds job to --config
Then uploads the program to the corresponding nodes
Then updates --config

How the job tracker tracks progress:
It gets all the jobs from --config. It then it asks the corresponding files for updates
*/
func (g *Group) GetJobs() ([]octavius.Topology, error) {
	reader, fileMissing, err := g.GetLatestFileReader("--config")
	if err != nil && !fileMissing {
		return nil, errors.New("unable to submit job")
	}

	jobs := make([]octavius.Topology, 0)
	if !fileMissing {
		dec := gob.NewDecoder(reader)
		for {
			var job octavius.Topology
			err = dec.Decode(&job)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal("config file corrupted: ", err)
			}
			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

func (g *Group) SubmitJob(topology octavius.Topology, program octavius.Program) (octavius.Topology, error) {
	g.jobLock.Lock()
	defer g.jobLock.Unlock()

	jobs, err := g.GetJobs()
	if err != nil {
		return nil, err
	}

	for _, job := range jobs {
		if octavius.CheckConflict(job, topology) {
			return nil, errors.New("job conflicts with existing job")
		}
	}
	newTopology, err := g.UploadTopology(topology, program)
	if err == nil {
		jobs = append(jobs, newTopology)
	}

	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	for _, job := range jobs {
		err := enc.Encode(job)
		if err != nil {
			log.Fatal("config file encoding failed")
		}
	}

	f := CreateTempFile(bytes.NewReader(buf.Bytes()), "--config", -1, BinaryFile)
	g.SendFile(f, "ALL")

	return newTopology, err
}

func (g *Group) DeleteJob(topology octavius.Topology) error {
	jobs, err := g.GetJobs()

	if err != nil {
		fmt.Println("Unable to get jobs right now")
	}

	found := false

	for _, job := range jobs {
		if job.Same(topology) {
			found = true
			break
		}
	}

	if !found {
		return errors.New("job not found")
	}

	for _, n := range topology {
		g.DeleteFunc(n.Name)
	}
	return nil
}

func (g *Group) GetJobStatus(topology octavius.Topology) error {
	return nil
}

func (g *Group) GetAllStatus() ([][]octavius.StatusSummary, error) {
	// First get all jobs
	jobs, err := g.GetJobs()

	if err != nil {
		fmt.Println("Unable to get jobs right now")
	}

	var result [][]octavius.StatusSummary
	for _, job := range jobs {
		statusSummary, err := g.GetTopologyStatus(job)
		if err != nil {
			return nil, err
		}
		result = append(result, statusSummary)
	}
	return result, nil
}

type Handler struct{ g *Group }

func (h Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	statusList, err := h.g.GetAllStatus()
	if err != nil {
		io.WriteString(w, fmt.Sprintf("Error 400: %v", err))
		return
	}

	io.WriteString(w, "<html><head><meta http-equiv='refresh' content='5' >"+
		"<link rel='stylesheet' "+
		"href='https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css' "+
		"integrity='sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm' "+
		"crossorigin='anonymous'></head>"+
		"<body>")
	io.WriteString(w, "<table class='table'>")
	for _, statuses := range statusList {
		io.WriteString(w, "<tr>"+
			"<th scope='col''>Name</th><th scope='col''>Completed</th>"+
			"<th scope='col''>Total</td><th scope='col'>Last Updated</th>")
		for _, status := range statuses {
			io.WriteString(w,
				fmt.Sprintf("<tr><td>%v</td><td>%v</td><td>%v</td><td>%v</td></tr>", status.Name,
					status.Completed, status.Total, status.Updated))
		}
		io.WriteString(w, "</tr><br/>")
	}
	io.WriteString(w, "</table>")
	io.WriteString(w, "</body></html>")

}

func (g *Group) StartStatusServer() {
	_, port := AddrToHostPort(g.Addr)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port+50000))
	if err != nil {
		log.Fatalf("Unable to set up HTTP Listener: %v", err)
	}

	port = listener.Addr().(*net.TCPAddr).Port
	log.Infof("Starting Status Server on Port %v", port)

	err = http.Serve(listener, Handler{g})
	if err != nil {
		log.Fatal(err)
	}
}

func (g *Group) GetTopologyStatus(topology octavius.Topology) ([]octavius.StatusSummary, error) {
	var result []octavius.StatusSummary
	log.Info("Received topology status request")
	for _, node := range topology {
		resp, err := g.GetStatus(node.Name)
		log.Info(resp)
		if err != nil {
			log.Info("Received msg: ", err)
			result = append(result, octavius.StatusSummary{node.Name, -1,
				-1, time.Time{}})
		} else {
			r := resp.Summarize()
			if node.Sink {
				r.Completed = r.Total
			}
			result = append(result, r)
		}
	}
	log.Info("Result: ", result)
	return result, nil
}

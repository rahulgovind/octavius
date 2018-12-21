package main

import (
	"fmt"
	"log"
	"time"
	"github.com/rahulgovind/octavius"
)

func main() {
	var topology = []octavius.Node{
		{
			Name:         "Spout",
			BlockSize:    1000000,
			InputFormat:  "line",
			OutputFormat: "json",
			Output:       []string{"Bolt"},
		},
		{
			Name:         "Bolt",
			InputFormat:  "json",
			OutputFormat: "json",
			Output:       []string{"Sink"},
		},
		{
			Name:        "Sink",
			InputFormat: "json",
			Sink:        true,
		},
	}

	var program = octavius.Program{Source: `
							package main
							import "strconv"

							func SpoutInput() interface{} {
								return ""
							}
	
							func SpoutOutput() interface{} {
								return int64(0)
							}

							func Spout(s string) []int64 { 
								i, _ := strconv.ParseInt(s, 10, 64)
								return []int64{i}
							}

							func BoltInput() interface{} {
								return int64(0)
							}

							func BoltOutput() interface{} {
								return int64(0)
							}

							func Bolt(v int64) []int64 {
								return []int64{2 * v}
							}

							func SinkInput() interface{} {
								return int64(0)
							}
							`}
	c := octavius.NewCrane("127.0.0.1:4101", topology, program)
	err := c.UploadProgram()
	if err != nil {
		log.Fatal("Unable to upload program")
	} else {
		fmt.Println("Job submit successful. You can exit and check updates on master website. 127.0.0.1:54001")
	}
	for {
		status, _ := c.GetTopologyStatus()
		for _, summary := range status {
			fmt.Printf("%v: Completed %v/%v.\t%v Left\n", summary.Name, summary.Completed, summary.Total,
				summary.Total-summary.Completed)
		}
		<-time.NewTimer(time.Second).C
	}
}

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rahulgovind/octavius"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

type Job struct {
	topology octavius.Topology
	program  octavius.Program
}

func main() {
	log.SetLevel(log.DebugLevel)
	app := cli.NewApp()
	app.Name = "Octavius Server"
	app.Usage = "Octavius Server and CLI"
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "coordinator",
			Usage: "Set for coordinators",
		},
		cli.StringFlag{
			Name:  "ip",
			Usage: "IP for current machine to listen on",
			Value: "127.0.0.1",
		},
		cli.IntFlag{
			Name:  "port, p",
			Usage: "Port current node should listen on. Lets kernel pick port if no port specific.",
			Value: 0,
		},
	}

	configFile, err := os.Open("config.json")
	if err != nil {
		log.Fatal("Unable to open config file: ", err)
	}
	buf := bytes.NewBuffer(nil)
	io.Copy(buf, configFile)

	var coordinators []string
	err = json.Unmarshal(buf.Bytes(), &coordinators)
	if err != nil {
		log.Fatal("Unable to decode configuration: ", err)
	}

	app.Action = func(c *cli.Context) error {
		isCoordinator := c.Bool("coordinator")
		ip := c.String("ip")
		port := c.Int("port")
		log.Debug("Port ", port)

		g := CreateGroup(&Config{
			Addr:          ip,
			IsCoordinator: isCoordinator,
			Port:          uint16(port),
			Coordinators:  coordinators,
		})

		if !isCoordinator {
			fmt.Println("Start")
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				text := strings.Split(scanner.Text(), " ")
				switch text[0] {
				case "put":
					localFilename := text[1]
					sdfsFilename := text[2]
					g.SendFunc(localFilename, sdfsFilename, "ONE")
				case "putjson":
					localFilename := text[1]
					sdfsFilename := text[2]
					g.SendJson(localFilename, sdfsFilename)
				case "get":
					sdfsFilename := text[1]
					localFilename := text[2]
					g.GetFunc(localFilename, sdfsFilename, "ALL")
				case "get-versions":
					sdfsFilename := text[1]
					numVersionsStr := text[2]
					localFilename := text[3]
					numVersions, _ := strconv.ParseInt(numVersionsStr, 10, 32)
					g.GetNFunc(localFilename, sdfsFilename, "ALL", int(numVersions))
				case "delete":
					sdfsFilename := text[1]
					g.DeleteFunc(sdfsFilename)
				case "ls":
					sdfsFilename := text[1]
					replicaList := g.FindReplicas(sdfsFilename)
					fmt.Println("Replicas:")
					for _, replica := range replicaList {
						fmt.Println(replica.Addr)
					}
				case "store":
					g.PrintStore()
				case "join":
					g.RequestMemberlist()
				case "print-list":
					g.PrintMembers()
				case "timestamp":
					fmt.Println(Timestamp())
				case "leave":
					g.Leave()
					g.Finish()
					return nil
				case "sps":
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
					}
					g.PrintTopologyStatus(topology)
				case "sp":
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
					_, err := g.SubmitJob(topology, program)
					if err != nil {
						fmt.Println(err)
					}
				case "reviews":
					var topology = []octavius.Node{
						{
							Name:         "ReviewsSpout",
							BlockSize:    10000,
							InputFormat:  "line",
							OutputFormat: "line",
							Output:       []string{"ReviewsTransform"},
						},
						{
							Name:         "ReviewsTransform",
							InputFormat:  "line",
							OutputFormat: "line",
							Output:       []string{"ReviewsFilter"},
						},
						{
							Name:         "ReviewsFilter",
							InputFormat:  "line",
							OutputFormat: "line",
							Output:       []string{"ReviewsSink"},
						},
						{
							Name:        "ReviewsSink",
							InputFormat: "line",
							Sink:        true,
						},
					}
					var program = octavius.Program{Source: `
						package main
						
						import "strings"
						
						func ReviewsSpoutInput() interface{} {
							return ""
						}
						
						func ReviewsSpoutOutput() interface{} {
							return ""
						}
						
						func ReviewsSpout(s string) []string {
							return []string{s}
						}
						
						func ReviewsTransformInput() interface{} {
							return ""
						}
						
						func ReviewsTransformOutput() interface{} {
							return ""
						}
						
						func ReviewsTransform(s string) []string {
							return []string{strings.ToLower(s)}
						}
						
						func ReviewsFilterInput() interface{} {
							return ""
						}
						
						func ReviewsFilterOutput() interface{} {
							return ""
						}
						
						func ReviewsSinkInput() interface{} {
							return ""
						}

						func ReviewsFilter(s string) []string {
							if strings.Contains(s, "iphone") {
								return []string{s}
							} else {
								return []string{}
							}
						}
						
						`}
					_, err := g.SubmitJob(topology, program)
					if err != nil {
						fmt.Println(err)
					}
				case "ping":
					var topology = []octavius.Node{
						{
							Name:         "PingSpout",
							InputFormat:  "line",
							OutputFormat: "line",
							BlockSize:    100000,
							Output:       []string{"PingFilterAndTransform"},
						},
						{
							Name:         "PingFilterAndTransform",
							InputFormat:  "line",
							OutputFormat: "json",
							Output:       []string{"PingSink"},
						},
						{
							Name:        "PingSink",
							InputFormat: "json",
							Sink:        true,
						},
					}
					var program = octavius.Program{Source: `
						package main

						import (
							"strconv"
							"strings"
						)

						func PingSpoutInput() interface{} {
							return ""
						}

						func PingSpoutOutput() interface{} {
							return ""
						}

						func getTimeString(s string) (result float32) {
							words := strings.Split(s, " ")
							result = -1
							for _, word := range words {
								if strings.HasPrefix(word, "time") {
									t, err := strconv.ParseFloat(word[5:], 32)
									if err == nil {
										result = float32(t)
										break
									}
								}
							}
							return
						}

						func PingSpout(s string) []string {
							return []string{s}
						}

						type PingData struct {
							S    string
							Time float32
							Type string
						}

						func PingFilterAndTransformInput() interface{} {
							return ""
						}

						func PingFilterAndTransformOutput() interface{} {
							return PingData{}
						}

						func PingFilterAndTransform(s string) []PingData {
							t := getTimeString(s)
							if t != -1 && t < 10 {
								return []PingData{}
							} else {
								if t == -1 {
									return []PingData{{s, t, "UNKNOWN"}}
								} else if t >= 10.0 && t < 20.0 {
									return []PingData{{s, t, "BAD"}}
								} else {
									return []PingData{{s, t, "FATAL"}}
								}
							}
						}

						func PingSinkInput() interface{} {
							return PingData{}
						}

					`}
					_, err := g.SubmitJob(topology, program)
					if err != nil {
						fmt.Println(err)
					}
				case "log":
					var topology = []octavius.Node{
						{
							Name:         "LogSpout",
							InputFormat:  "line",
							OutputFormat: "json",
							BlockSize:    10000,
							Output:       []string{"LogFilter"},
						},
						{
							Name:         "LogFilter",
							InputFormat:  "json",
							OutputFormat: "json",
							Output:       []string{"LogTransform"},
						},
						{
							Name:         "LogTransform",
							InputFormat:  "json",
							OutputFormat: "json",
							Output:       []string{"LogJoin"},
						},
						{
							Name:         "LogJoin",
							InputFormat:  "json",
							OutputFormat: "json",
							Output:       []string{"LogSink"},
						},
						{
							Name:        "LogSink",
							InputFormat: "json",
							Sink:        true,
						},
					}
					program := octavius.Program{Source: `
						package main
						
						import "strings"
						
						func LogSpoutInput() interface{} {
							return ""
						}
						
						func LogSpoutOutput() interface{} {
							return ""
						}
						
						func LogSpout(s string) []string {
							return []string{s}
						}
						
						func LogFilterInput() interface{} {
							return ""
						}
						
						func LogFilterOutput() interface{} {
							return ""
						}
						
						func LogFilter(s string) []string {
							if strings.Contains(s, "GET") && strings.Index(s, "GET")+5 < len(s) {
								return []string{s}
							}
							return []string{}
						}
						
						func LogTransformInput() interface{} {
							return ""
						}
						
						func LogTransformOutput() interface{} {
							return ""
						}
						
						func LogTransform(s string) []string {
							i := strings.Index(s, "GET")
							suffix := s[i+4:]
							spaceIndex := len(suffix)
							idx := strings.Index(suffix, " ")
							if idx >= 0 {
								spaceIndex = idx
							}
							return []string{suffix[:spaceIndex]}
						}
						
						type JoinOutput struct {
							URL      string
							Category string
						}
						
						func LogJoinInput() interface{} {
							return ""
						}
						
						func LogJoinOutput() interface{} {
							return JoinOutput{}
						}
						
						func LogJoin(s string) []JoinOutput {
							var result []JoinOutput
							categories := strings.Split(s, "/")
							for _, category := range categories {
								if category != "" {
									result = append(result, JoinOutput{s, category})
								}
							}
							return result
						}
						
						func LogSinkInput() interface{} {
							return JoinOutput{}
						}

						`}
					_, err := g.SubmitJob(topology, program)
					if err != nil {
						fmt.Println(err)
					}
				case "status-all":
					g.GetAllStatus()
				case "programs":
					g.PrintRegisteredPrograms()
				case "queue":
					fmt.Println(executionQueue.Len())
				case "status":
					sdfsFilename := text[1]
					g.PrintStatus(sdfsFilename)
				case "count":
					filename := text[1]
					fmt.Println(g.CountTuples(filename))
				case "sockstream":
					filename := text[1]
					port := 9999
					go g.LaunchSockStream(filename, port, 2*time.Second)
				default:
					fmt.Println("Unexpected command")
				}
				fmt.Println("Done")
			}
		} else {
			g.Finish()
		}
		return nil
	}

	err = app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

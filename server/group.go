package main

import (
	"fmt"
	"github.com/rahulgovind/octavius"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
)

const (
	GossipB              uint32 = 2
	GossipT              uint32 = 20
	GossipTTL            uint32 = 5
	FailureCycle                = 500 * time.Millisecond
	FailureTimeout              = 500 * time.Millisecond
	RecheckCount                = 1
	ReplicationFactor           = 3
	NumSuccessors               = 3
	M                           = 0x3fffffffffffffff
	MaxConcurrentUploads        = 10
	NumExecutors                = 4
	NumFileHandlers             = 1
)

// Group public methods
type Group struct {
	coordinators       []string
	coordinatorRPCs    []string
	primaryCoordinator bool
	Addr               string
	RPCAddr            string
	Incarnation        uint64
	isCoordinator      bool
	exitOnce           sync.Once
	counter            AtomicCounter
	sem                *Semaphore
	broadcaster        *Gossiper
	wg                 sync.WaitGroup
	handlerStopChan    chan bool
	detector           *FailureDetector
	leaving            bool
	joined             bool
	pr                 *ProgramRegistry // Maintains list of programs
	fileChan           chan<- File      // All files pushed to fileChan are uploaded to cluster
	enforced           map[File]bool
	jobLock            sync.Mutex
	State
}

// CreateGroup creates a group with the given configuration.
// It automatically retrieves the membership list from the introducer and starts the command handler
// for the current node
func CreateGroup(config *Config) *Group {
	incarnation := uint64(time.Now().Unix())
	rand.Seed(time.Now().UTC().UnixNano() + int64(incarnation))

	addr := config.Addr
	port := config.Port
	coordinators, coordinatorRPCs := resolveCoordinators(config.Coordinators)

	rpcAddr := fmt.Sprintf("%v:%v", addrToIP(addr), port+100)
	addr = fmt.Sprintf("%v:%v", addrToIP(addr), port)
	verifyAddresses(coordinators, addr, config.IsCoordinator)

	var absDir string
	var members []Member
	// Setup file system and initial membership list
	// File system
	workingDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	dir := fmt.Sprintf("%v", port)
	absDir = path.Join(workingDir, dir)
	baseAddr, _ := AddrToHostPort(addr)
	octavius.InitFileSystem(absDir, baseAddr)
	if !config.IsCoordinator {
		// Initial membership list
		members = append(members, Member{
			Addr:        addr,
			RPCAddr:     rpcAddr,
			Incarnation: incarnation,
		})
	}

	msgch := make(chan *Message, 100)
	go ListenUDP(addr, msgch)
	go ListenTCP(addr, msgch)

	g := &Group{
		coordinators:       coordinators,
		coordinatorRPCs:    coordinatorRPCs,
		primaryCoordinator: addr == coordinators[0],
		Addr:               addr,
		isCoordinator:      config.IsCoordinator,
		Incarnation:        incarnation,
		sem:                NewSemaphore(MaxConcurrentUploads),
		broadcaster:        nil,
		detector:           nil,
		handlerStopChan:    make(chan bool, 1),
		State:              *NewState(Member{Addr: addr, RPCAddr: rpcAddr, Incarnation: incarnation}),
		RPCAddr:            rpcAddr,
	}

	// Worker-specific tasks
	if !g.isCoordinator {
		g.pr = NewProgramRegistry()
		fileChan := make(chan File, 10240)
		g.fileChan = fileChan
		g.StartFileHandlers(NumFileHandlers, fileChan)
		StartExecutors(NumExecutors, fileChan)
	}

	// Needed by workers and coordinators
	g.broadcaster = NewGossiper(g, msgch)
	g.detector = NewFailureDetector(g)
	g.wg.Add(1)
	go g.MessageHandler(msgch)
	go g.broadcaster.Handler()

	if g.isCoordinator {
		g.RequestMemberlist()
		go g.RPCServerListener()
		go g.StartStatusServer()
	}
	return g
}

func verifyAddresses(coordinators []string, addr string, isCoordinator bool) {
	log.Debug("Coordinators: ", coordinators)
	log.Debug("Current addr: ", addr)
	inList := false
	for _, coordinator := range coordinators {
		if coordinator == addr {
			inList = true
			break
		}
	}

	if isCoordinator && !inList {
		log.Fatal("Address not in configuration list. Invalid coordinator")
	}

	if !isCoordinator && inList {
		log.Fatal("Address belongs to coordinator. Invalid worker")
	}
}

func (g *Group) inCoordinators(addr string) bool {
	inList := false
	for _, coordinator := range g.coordinators {
		if coordinator == addr {
			inList = true
			break
		}
	}
	return inList
}
func resolveCoordinators(addr []string) ([]string, []string) {
	var result []string
	var result2 []string
	for _, s := range addr {
		ip, port := AddrToHostPort(s)
		result = append(result, fmt.Sprintf("%v:%v", addrToIP(ip), port))
		result2 = append(result2, fmt.Sprintf("%v:%v", addrToIP(ip), port+100))
	}
	return result, result2
}

// RequestMemberlist attempts to get membership list from introducer
func (g *Group) RequestMemberlist() {
	joinMessage := &JoinMessage{
		Member: g.Member().ProtoMember(),
	}
	protoJoinMessage, _ := ptypes.MarshalAny(joinMessage)

	m := &Message{
		MessageType: Message_INTRO_SYN,
		Addr:        g.Addr,
		Details:     protoJoinMessage,
	}

	// Only join using primary coordinator
	for _, coordinator := range g.coordinators {
		if coordinator == g.Addr {
			continue
		}
		SendTCP(coordinator, m)
	}
}

// HandleJoin starts membership / failure detection once the cluster has been started.
func (g *Group) HandleJoin(members []Member) {
	if g.joined {
		log.Infof("Already part of cluster. Cannot join another one")
		return
	}

	g.SetMembers(members)
	g.joined = true

	// We only join after we are officially part of a cluster.
	// This was done to resolve problems present before where the introducer introduced the
	// new node to every one else. But it shou
	if !g.isCoordinator {
		go g.RPCServerListener()
		go g.detector.Handler(time.NewTicker(FailureCycle), FailureTimeout)

		g.introduce()
	}
}

// introduce is run by the new guy to introduce a himself to everyone
func (g *Group) introduce() {
	protoMember := g.Member().ProtoMember()
	introMsg := &IntroMessage{Member: protoMember}
	protoIntroMsg, err := ptypes.MarshalAny(introMsg)
	if err != nil {
		log.Fatal("Unable to create introduction message")
		return
	}

	msg := &Message{
		MessageType: Message_ADD,
		Addr:        g.Addr,
		Details:     protoIntroMsg,
	}
	for _, coordinator := range g.coordinators {
		SendTCP(coordinator, msg)
	}
	g.broadcaster.Broadcast(msg)
}

func (g *Group) Leave() {
	g.leaving = true
	g.broadcastFailure(*g.Member())
	go func() { g.GracefulExit() }()
}

// For a node (non-introducer) to exit gracefully. It must
// 1) Broadcast a leave message.
// 2) Continue to process incoming broadcasts and send broadcasts for another T_wait1 seconds
// 3) Finish sending broadcasts or exit in T_wait2 seconds
func (g *Group) GracefulExit() {
	g.exitOnce.Do(func() {
		g.detector.Stop()
		g.stopMessageHandler()
		g.wg.Done()
	})
}

// Member returns current process as a member
func (g *Group) Member() *Member {
	return &Member{
		Addr:        g.Addr,
		RPCAddr:     g.RPCAddr,
		Incarnation: g.Incarnation,
	}
}

func (g *Group) Finish() {
	g.wg.Wait()
}

func (g *Group) AddFile(f File) (bool, bool, bool) {
	rightReplica, alreadyCommitted, inPrimary := g.CommitFile(f.Copy())

	if (rightReplica && !alreadyCommitted) || !rightReplica {
		f.LogPrint()
		g.fileChan <- f.Copy()
		if inPrimary {
			g.RunProgram(f.Copy())
		}
	}
	f.Delete()
	return rightReplica, alreadyCommitted, inPrimary
}

func (g *Group) StartFileHandlers(numHandlers int, fileChan <-chan File) {
	for i := 0; i < numHandlers; i++ {
		go g.FileStreamHandler(fileChan)
	}
}

func (g *Group) AddMember(member Member) {
	if g.isCoordinator {
		g.ml.Add(member)
		return
	}

	if !g.joined {
		return
	}

	files, added, _ := g.addMember(member)
	if !added {
		return
	}

	for _, f := range files {
		g.fileChan <- f.Copy()
		f.Delete()
	}
}

func (g *Group) RemoveMember(member Member) {
	if g.isCoordinator {
		g.ml.Remove(member)
		return
	}

	if !g.joined {
		return
	}

	files, removed, primaryDiff := g.removeMember(member)
	if !removed {
		return
	}

	log.Debug("Primary Diff", primaryDiff)
	for _, f := range files {
		g.fileChan <- f.Copy()
		if primaryDiff.InRange(f.Id()) {
			g.RunProgram(f.Copy())
		}
		f.Delete()
	}
}

func (g *Group) SetMembers(members []Member) {
	if g.isCoordinator {
		g.ml.Set(members)
		return
	}
	files, primaryRange := g.setMembers(members)
	for _, f := range files {
		g.fileChan <- f.Copy()
		if primaryRange.InRange(f.Id()) {
			g.RunProgram(f.Copy())
		}
		f.Delete()
	}
}

func (g *Group) MarkFail(member Member) {
	g.broadcastFailure(member)
	g.RemoveMember(member)
}

func (g *Group) broadcastFailure(member Member) {
	log.Debugf("Broadcasting failure message for %v", member)
	suspectMsg := &SuspectMessage{
		Suspect: member.ProtoMember(),
		Ts:      uint64(time.Now().Unix()),
	}

	protoSuspectMsg, _ := ptypes.MarshalAny(suspectMsg)
	msg := &Message{
		MessageType: Message_SUSPECT,
		Addr:        g.Addr,
		Details:     protoSuspectMsg,
	}

	g.broadcaster.Broadcast(msg)
	for _, coordinator := range g.coordinators {
		SendTCP(coordinator, msg)
	}
}

func (g *Group) FileStreamHandler(fileChan <-chan File) {
	for {
		f := <-fileChan
		g.EnforceConsistency(f)
	}
}

func (g *Group) RunProgram(f File) {
	g.pr.ProcessFile(f, g.fr)
}

func (g *Group) EnforceConsistency(f File) {
	var tryAgain bool
	for {
		log.Debugf("Enforcing consistency on %v:%v:\t%v\n", f.File, f.Sequence, f.AutoTombstone)
		tryAgain = false

		replicas := g.FindReplicas(f.File)
		for _, replica := range replicas {
			log.Debugf("Enforcing consistency for file %v on %v", f, replica.Addr)
			log.Debugf("Replica: %v", replica)
			success, err := g.UploadFile(f, replica)

			if !success || err != nil {
				tryAgain = true
				break
			}
		}
		if tryAgain {
			time.Sleep(15 * time.Millisecond)
		} else {
			break
		}
	}
	f.Delete()
}

func (g *Group) CountTuples(filename string) int {
	return g.pr.Count(filename, g.fr.Find(filename))
}
func (g *Group) PrintRegisteredPrograms() {
	g.pr.PrintRegisteredPrograms()
}

func (g *Group) PrintStatus(filename string) {
	resp, err := g.GetStatus(filename)
	if err != nil {
		fmt.Printf("Job not found: %v\n", err)
		return
	}
	s := resp.Summarize()
	fmt.Printf("%v: Completed %v/%v.\t%v Left\n", filename, s.Completed, s.Total, s.Total-s.Completed)
}

func (g *Group) PrintTopologyStatus(topology octavius.Topology) {
	resp, err := octavius.GetTopologyStatus(g.coordinatorRPCs[0], topology)
	if err != nil {
		fmt.Println("Unable to get job status: ", err)
	}
	for _, summary := range resp {
		fmt.Printf("%v: Completed %v/%v.\t%v Left\n", summary.Name, summary.Completed, summary.Total,
			summary.Total-summary.Completed)
	}
}

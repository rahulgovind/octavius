package octavius

import (
	log "github.com/sirupsen/logrus"
	"net/rpc"
	"sync"
	"time"
)

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

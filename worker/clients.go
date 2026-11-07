package worker

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gabrielelanaro/queng/workerapi"
	"google.golang.org/grpc"
)

// Clients caches connections to workers. Close it when the worker process stops.
type Clients struct {
	mu          sync.Mutex
	connections map[string]*grpc.ClientConn
	options     []grpc.DialOption
	closed      bool
}

// NewClients configures connections to producer workers.
func NewClients(options ...grpc.DialOption) *Clients {
	return &Clients{connections: make(map[string]*grpc.ClientConn), options: options}
}

// Get returns a client backed by the cached connection for address.
func (c *Clients) Get(address string) (workerapi.WorkerClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("worker clients are closed")
	}
	conn := c.connections[address]
	if conn == nil {
		var err error
		conn, err = grpc.NewClient("passthrough:///"+address, c.options...)
		if err != nil {
			return nil, err
		}
		c.connections[address] = conn
	}
	return workerapi.NewWorkerClient(conn), nil
}

// Close closes the cached connections.
func (c *Clients) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var errs []error
	for _, conn := range c.connections {
		errs = append(errs, conn.Close())
	}
	c.connections = nil
	return errors.Join(errs...)
}

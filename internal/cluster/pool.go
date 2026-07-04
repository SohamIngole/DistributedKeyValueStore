package cluster

import (
	"net"
	"sync"
	"time"
)

// Manages a pool of reusable TCP connections to a single node
type ConnPool struct {
    mu      sync.Mutex
    addr    string
    idle    []net.Conn
  	maxIdle int
}

func NewConnPool(addr string, maxIdle int) *ConnPool {
    return &ConnPool{addr: addr, maxIdle: maxIdle}
}

// Lend a connection to callers
func (p *ConnPool) Get() (net.Conn, error) {
    p.mu.Lock()
    if len(p.idle) > 0 {
        conn := p.idle[len(p.idle)-1]
        p.idle = p.idle[:len(p.idle)-1] // Taking the last connection then decreasing length by 1. O(1)
        p.mu.Unlock()
        return conn, nil
    }
    p.mu.Unlock()
    return net.DialTimeout("tcp", p.addr, 2*time.Second) // Creating a new connection (No idle connection)
}

// Puts connection into the pool
func (p *ConnPool) Put(conn net.Conn) {
	if conn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if (len(p.idle) > p.maxIdle) {
		conn.Close() // pool is full
		return
	}
	p.idle = append(p.idle, conn)
}
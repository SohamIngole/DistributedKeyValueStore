package replication

import (
    "bufio"
    "fmt"
    "log"
    "net"
    "sync"
	"strings"

	"DistributedKeyValueStore/internal/resp"
)

// Manages a set of connected replicas and fans out write commands
type Primary struct {
    mu       sync.RWMutex
    replicas map[string]net.Conn  // Replica addr -> connection
}

func NewPrimary() *Primary {
    return &Primary{replicas: make(map[string]net.Conn)}
}


// Starts a listener on replPort for incoming replica connections
func (p *Primary) ListenForReplicas(replPort string) {
    ln, err := net.Listen("tcp", replPort)
    if err != nil {
        log.Fatalf("replication listener: %v", err)
    }
    log.Printf("replication port open at %s", replPort)
    for {
        conn, err := ln.Accept()
        if err != nil {
            continue
        }
        go p.handleReplica(conn)
    }
}

func (p *Primary) handleReplica(conn net.Conn) {
    addr := conn.RemoteAddr().String() // ephimeral port, not listening port
    r := bufio.NewReader(conn)

    // Expect REPLCONF handshake
    args, err := resp.ReadCommand(r)
    if err != nil || len(args) == 0 || strings.ToUpper(args[0]) != "REPLCONF" {
        log.Printf("replica %s sent bad handshake", addr)
        conn.Close()
        return
    }
    fmt.Fprint(conn, "+OK\r\n")

    p.mu.Lock()
    p.replicas[addr] = conn
    p.mu.Unlock()

    log.Printf("replica %s connected", addr)

    // Detect disconnect
    buf := make([]byte, 1)
    for {
        _, err := conn.Read(buf)
        if err != nil {
            log.Printf("replica %s disconnected", addr)
            p.mu.Lock()
            delete(p.replicas, addr)
            p.mu.Unlock()
            conn.Close()
            return
        }
    }
}

// Sends a write command to all connected replicas
// Called after every successful write on the primary
func (p *Primary) Propagate(args []string) {
    p.mu.RLock()
    defer p.mu.RUnlock()

    // Serialize once, send to all
    var buf strings.Builder
    fmt.Fprintf(&buf, "*%d\r\n", len(args))
    for _, a := range args {
        fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(a), a)
    }
    serialized := buf.String()

    var dead []string
    for addr, conn := range p.replicas {
        if _, err := fmt.Fprint(conn, serialized); err != nil {
            log.Printf("failed to propagate to replica %s: %v", addr, err)
            dead = append(dead, addr)
        }
    }

    // Clean up dead replicas
    if len(dead) > 0 {
        p.mu.RUnlock()
        p.mu.Lock()
        for _, addr := range dead {
            delete(p.replicas, addr)
        }
        p.mu.Unlock()
        p.mu.RLock()
    }
}

func (p *Primary) Close() {
    p.mu.Lock()
    defer p.mu.Unlock()
    for addr, conn := range p.replicas {
        conn.Close()
        delete(p.replicas, addr)
    }
}
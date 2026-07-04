package coordinator

import (
    "net"
    "sync"
    "time"
	"log"
	"bufio"
	"strings"
	"fmt"
)

type healthChecker struct {
    coord    *Coordinator
    interval time.Duration
    dead     sync.Map  // map[nodeAddr]bool
}

func newHealthChecker(c *Coordinator, interval time.Duration) *healthChecker {
    return &healthChecker{coord: c, interval: interval}
}

func (h *healthChecker) Start() {
    go func() {
        ticker := time.NewTicker(h.interval)
        for range ticker.C {
            for _, addr := range h.coord.ring.Nodes() {
                go h.check(addr)
            }
        }
    }()
}

func (h *healthChecker) check(addr string) {
    conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
    if err != nil {
        if _, dead := h.dead.LoadOrStore(addr, true); !dead {
            log.Printf("health: node %s is DOWN — removing from ring", addr)
            h.coord.ring.RemoveNode(addr)
        }
        return
    }
    // Send PING
    fmt.Fprint(conn, "*1\r\n$4\r\nPING\r\n")
    r := bufio.NewReader(conn)
    line, _ := r.ReadString('\n')
    conn.Close()

    if strings.Contains(line, "PONG") {
        if _, wasDead := h.dead.LoadAndDelete(addr); wasDead {
            log.Printf("health: node %s is back UP — adding to ring", addr)
            h.coord.ring.AddNode(addr)
        }
    }
}
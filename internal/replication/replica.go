package replication

import (
    "bufio"
    "log"
    "net"
    "strings"
    "time"
	"fmt"
	"strconv"

	"DistributedKeyValueStore/internal/store"
	"DistributedKeyValueStore/internal/resp"
)

// Connects to primaryAddr and streams incoming write commands, applying them to store s
func StartReplica(primaryAddr string, s *store.Store) {
    for {
        err := connectAndStream(primaryAddr, s)
        if err != nil {
            log.Printf("replication error: %v. Retrying in 3s", err)
        }
        time.Sleep(3 * time.Second)
    }
}

func connectAndStream(primaryAddr string, s *store.Store) error {
    conn, err := net.DialTimeout("tcp", primaryAddr, 5*time.Second)
    if err != nil {
        return fmt.Errorf("dial primary: %w", err)
    }
    defer conn.Close()

    // Send REPLCONF handshake
    fmt.Fprint(conn, "*1\r\n$8\r\nREPLCONF\r\n")

    r := bufio.NewReader(conn)
    ack, err := r.ReadString('\n')
    if err != nil || !strings.Contains(ack, "OK") {
        return fmt.Errorf("handshake failed: %q", ack)
    }
    log.Printf("replica: connected to primary at %s", primaryAddr)

    // Stream and apply commands
    for {
        args, err := resp.ReadCommand(r)
        if err != nil {
            return fmt.Errorf("stream error: %w", err)
        }
        if len(args) == 0 {
            continue
        }
        applyToStore(s, args)
    }
}

func applyToStore(s *store.Store, args []string) {
    switch strings.ToUpper(args[0]) {
    case "SET":
        if len(args) >= 3 {
            s.Set(args[1], args[2], 0)
        }
    case "DEL":
        if len(args) >= 2 {
            s.Delete(args[1:]...)
        }
    case "EXPIRE":
        if len(args) == 3 {
            secs, _ := strconv.ParseInt(args[2], 10, 64)
            s.Expire(args[1], time.Duration(secs)*time.Second)
        }
    }
}
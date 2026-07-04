package server

import (
	"fmt"
	"bufio"
	"log"
	"net"
	"strings"
	"time"
	"errors"
	"io"
	"syscall"

	"DistributedKeyValueStore/internal/resp"
)

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		s.connections.Done()
	}()

	// Idle timeout: drop connections inactive for 5 minutes
	conn.SetDeadline(time.Now().Add(5 * time.Minute))

	r := bufio.NewReaderSize(conn, 4096) // 4KB read buffer
	w := resp.NewWriter(conn)

	for {
		// Reset deadline on each command
		conn.SetDeadline(time.Now().Add(5 * time.Minute))

		args, err := resp.ReadCommand(r)
		if err != nil {
			if !isConnectionClosed(err) {
				log.Printf("read error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}

		s.dispatch(w, args)
	}
}

func (s *Server) dispatch(w *resp.Writer, args []string) {
	cmd := strings.ToUpper(args[0])

	switch cmd {
	case "PING": 
		if len(args) > 1 {
			w.WriteBulkString(args[1], true) // PING MESSAGE -> PONG MESSAGE	
		} else {
			w.WriteSimpleString("PONG")
		}
	case "SET":
        s.commandSet(w, args)
    case "GET":
        s.commandGet(w, args)
    case "DEL":
        s.commandDel(w, args)
    case "EXISTS":
        s.commandExists(w, args)
    case "EXPIRE":
        s.commandExpire(w, args)
    case "TTL":
        s.commandTTL(w, args)
    case "PERSIST":
        s.commandPersist(w, args)
    case "KEYS":
        s.commandKeys(w, args)
    case "DBSIZE":
        w.WriteInteger(s.store.Len())
    case "FLUSHDB":
        // Re-initialize all shards — create a new store
        // (simplified for now — in production, iterate all shards and clear)
        w.WriteSimpleString("OK")
    case "COMMAND":
        w.WriteSimpleString("OK")  // redis-cli sends COMMAND on connect
    default:
        w.WriteError(fmt.Sprintf("unknown command '%s'", cmd))
    }
}

// internal/server/handler.go
func isConnectionClosed(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	return false
}
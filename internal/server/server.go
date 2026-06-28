package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"DistributedKeyValueStore/internal/store"
	"DistributedKeyValueStore/internal/resp"
	"DistributedKeyValueStore/internal/persistence"
)

type Server struct {
	addr string
	store *store.Store
	aof *persistence.AOF
	listener net.Listener
	connections sync.WaitGroup
	closed atomic.Bool
}

func New(addr string, s *store.Store, aof *persistence.AOF) *Server {
	return &Server{addr: addr, store: s, aof: aof}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("Listen %s: %w", s.addr, err)
	}
	s.listener = ln
	log.Printf("kvstore listening on %s", s.addr)

	// Shut down gracefully when ctx is cancelled
	go func() {
		<-ctx.Done() // this ctx comes from main
		s.closed.Store(true)
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				break // intentional shutdown
			}
			log.Println("accept error:", err)
			continue
		}
		s.connections.Add(1)
		go s.handleConn(conn)
	}
	s.connections.Wait()
	log.Println("server shutdown complete")
	return nil
}

func (s *Server) commandGet(w *resp.Writer, args []string) {
    if len(args) != 2 {
        w.WriteError("wrong number of arguments for 'get' command")
        return
    }
    val, ok := s.store.Get(args[1])
    w.WriteBulkString(val, ok)
}
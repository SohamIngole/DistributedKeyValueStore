package integration_test

import (
    "context"
    "fmt"
    "net"
    "testing"
    "time"
	"bufio"
	"strings"
	"sync"

    "DistributedKeyValueStore/internal/persistence"
    "DistributedKeyValueStore/internal/server"
    "DistributedKeyValueStore/internal/store"
)

// spins up a real server on a random port and returns a client connection.
func startTestServer(t *testing.T) net.Conn {
	t.Helper()

	s := store.New()
	aof, err := persistence.NewAOF(t.TempDir()+"/test.aof", persistence.SyncNever)
	if err != nil {
		t.Fatalf("create test AOF: %v", err)
	}
	t.Cleanup(func() { aof.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	addr := fmt.Sprintf("127.0.0.1:%d", findFreePort(t))
	srv := server.New(addr, s, aof)
	go srv.ListenAndServe(ctx)

	// wait until the server is ready (until the server has finished callin net.Listen)
	var conn net.Conn
	for i := 0; i < 100; i++ {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("connect to test server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func findFreePort(t *testing.T) int {
    t.Helper()
    l, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        t.Fatal(err)
    }
    port := l.Addr().(*net.TCPAddr).Port
    l.Close()
    return port
}

// sends a raw RESP command and returns the raw response line
func sendRecv(t *testing.T, conn net.Conn, cmd string) string {
    t.Helper()
    fmt.Fprint(conn, cmd)
    r := bufio.NewReader(conn)
    line, _ := r.ReadString('\n')
    return strings.TrimRight(line, "\r\n")
}

func TestSetGet_Integration(t *testing.T) {
    conn := startTestServer(t)

    resp := sendRecv(t, conn, "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nAlice\r\n")
    if resp != "+OK" {
        t.Errorf("SET: got %q, want +OK", resp)
    }

    resp = sendRecv(t, conn, "*2\r\n$3\r\nGET\r\n$4\r\nname\r\n")
}

// Race condition tests
func TestConcurrentSetGet(t *testing.T) {
    s := store.New()
    var wg sync.WaitGroup
    const workers = 50
    const opsPerWorker = 1000

    for i := 0; i < workers; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for j := 0; j < opsPerWorker; j++ {
                key := fmt.Sprintf("key:%d", id*opsPerWorker+j)
                s.Set(key, "value", 0)
                s.Get(key)
                if j%100 == 0 {
                    s.Delete(key)
                }
            }
        }(i)
    }
    wg.Wait()
}

// TTL edge case tests
func TestTTLEdgeCases(t *testing.T) {
    t.Run("expire_in_past", func(t *testing.T) {
        s := store.New()
        s.Set("k", "v", 1*time.Nanosecond)
        time.Sleep(1 * time.Millisecond)
        _, ok := s.Get("k")
        if ok {
            t.Error("key with 1ns TTL should be expired after 1ms")
        }
    })

    t.Run("persist_removes_ttl", func(t *testing.T) {
        s := store.New()
        s.Set("k", "v", 100*time.Millisecond)
        s.Persist("k")
        time.Sleep(200 * time.Millisecond)
        _, ok := s.Get("k")
        if !ok {
            t.Error("key should survive after Persist() removes TTL")
        }
    })

    t.Run("expire_then_set_clears_ttl", func(t *testing.T) {
        s := store.New()
        s.Set("k", "v", 50*time.Millisecond)
        s.Set("k", "v2", 0)  // overwrite with no TTL
        time.Sleep(100 * time.Millisecond)
        _, ok := s.Get("k")
        if !ok {
            t.Error("re-SET without TTL should clear the previous TTL")
        }
    })
}
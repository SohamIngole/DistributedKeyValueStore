package persistence

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
	"io"
	"log"

	"DistributedKeyValueStore/internal/resp"
	"DistributedKeyValueStore/internal/store"
)

// SyncPolicy controls when data is flushed to disk
type SyncPolicy int

const (
    SyncAlways  SyncPolicy = iota  // fsync on every write (safest)
    SyncEverySecond // background fsync each second (default)
    SyncNever // let the OS decide (fastest)
)

type AOF struct {
	mu sync.Mutex
	file *os.File
	buf *bufio.Writer
	policy SyncPolicy
}

func NewAOF(path string, policy SyncPolicy) (*AOF, error) {
	f, err  := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("aof open %s: %w", path, err)
	}

    a := &AOF{file: f, buf: bufio.NewWriterSize(f, 32*1024), policy: policy} // Write buffer of size 32KB. Buffering batches up writes in memory first
    if policy == SyncEverySecond {
        go a.backgroundSync()
    }
    return a, nil
}

// Serializes args into RESP format and writes to the AOF buffer.	
func (a *AOF) Append(args []string) error {
    a.mu.Lock()
    defer a.mu.Unlock()

    // Write RESP array
    fmt.Fprintf(a.buf, "*%d\r\n", len(args))
    for _, arg := range args {
        fmt.Fprintf(a.buf, "$%d\r\n%s\r\n", len(arg), arg)
    }

    switch a.policy {
    case SyncAlways:
        if err := a.buf.Flush(); err != nil {
            return err
        }
        return a.file.Sync()    // fsync to disk
    case SyncEverySecond:
        // backgroundSync handles flushing — just accumulate
        return nil
    case SyncNever:
        return nil
    }
    return nil
}

func (a *AOF) backgroundSync() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
    for range ticker.C {
        a.mu.Lock()
        a.buf.Flush()
        a.file.Sync()
        a.mu.Unlock()
    }
}

// Replay reads all commands from the AOF and calls handler for each.
// Used at startup to reconstruct store state.
func (a *AOF) Replay(handler func(args []string) error) error {
    // Seek to beginning
    if _, err := a.file.Seek(0, 0); err != nil {
        return err
    }
    r := bufio.NewReader(a.file)
    for {
        args, err := resp.ReadCommand(r)
        if err == io.EOF {
            break
        }
        if err != nil {
            // Truncated write (crash mid-write) — stop here
            log.Printf("aof: stopping replay at corrupt entry: %v", err)
            break
        }
        if len(args) > 0 {
            if err := handler(args); err != nil {
                return err
            }
        }
    }
    return nil
}

// Rewrite creates a new, minimal AOF from current store state.
// Eliminates redundant entries (e.g., multiple SETs to the same key).
func (a *AOF) Rewrite(store *store.Store, newPath string) error {
    f, err := os.Create(newPath)
    if err != nil {
        return err
    }
    defer f.Close()

    w := bufio.NewWriterSize(f, 64*1024)
    keys := store.Keys("*")
    for _, key := range keys {
        val, ok := store.Get(key)
        if !ok {
            continue
        }
        // Serialize as SET key value
        fmt.Fprintf(w, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
            len(key), key, len(val), val)
    }
    if err := w.Flush(); err != nil {
        return err
    }

    // Atomically replace the old AOF with the new one
    a.mu.Lock()
    defer a.mu.Unlock()
    return os.Rename(newPath, a.file.Name())
}

func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.buf.Flush()
	a.file.Sync()
	return a.file.Close()
}
package persistence_test

import (
	"os"
	"testing"
	"strings"
	"path/filepath"

	"DistributedKeyValueStore/internal/store"
	"DistributedKeyValueStore/internal/persistence"
)

func TestReplayRestoresState(t *testing.T) {
    tmp := t.TempDir()
    path := filepath.Join(tmp, "test.aof")

    aof, err := persistence.NewAOF(path, persistence.SyncAlways)
    if err != nil {
        t.Fatal(err)
    }
    aof.Append([]string{"SET", "name", "Alice"})
    aof.Append([]string{"SET", "city", "Mumbai"})
    aof.Append([]string{"DEL", "city"})
    aof.Close()

    // Replay into a new store
    s := store.New()
    aof2, err := persistence.NewAOF(path, persistence.SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { aof2.Close() })

    aof2.Replay(func(args []string) error {
        if strings.ToUpper(args[0]) == "SET" {
            s.Set(args[1], args[2], 0)
        } else if strings.ToUpper(args[0]) == "DEL" {
            s.Delete(args[1:]...)
        }
        return nil
    })

    val, ok := s.Get("name")
    if !ok || val != "Alice" {
        t.Errorf("expected name=Alice, got %q ok=%v", val, ok)
    }
    if s.Exists("city") {
        t.Error("city should have been deleted")
    }
}

func TestCorruptedAOFReplayIsSafe(t *testing.T) {
    tmp := t.TempDir()
    path := filepath.Join(tmp, "corrupt.aof")

    // Write valid commands, then append garbage (simulates crash mid-write)
    f, _ := os.Create(path)
    f.WriteString("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
    f.WriteString("*3\r\n$3\r\nSET\r\n$CORRUPT")  // truncated entry
    f.Close()

    s := store.New()
    aof, err := persistence.NewAOF(path, persistence.SyncNever) // sync policy is irrelevant as this is a read-only task
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { aof.Close() })

    err = aof.Replay(func(args []string) error {
        if strings.ToUpper(args[0]) == "SET" {
            s.Set(args[1], args[2], 0)
        }
        return nil
    })

    if err != nil {
        t.Errorf("replay should be fault-tolerant, got: %v", err)
    }

    // The good command before the corruption should have been applied
    if val, ok := s.Get("foo"); !ok || val != "bar" {
        t.Errorf("expected foo=bar, got %q ok=%v", val, ok)
    }
}
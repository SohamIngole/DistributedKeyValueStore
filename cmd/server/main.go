package main

import (
    "context"
    "flag"
    "log"
    "os"
    "os/signal"
    "strings"
    "syscall"
	"time"
	"strconv"

	"DistributedKeyValueStore/internal/persistence"
	"DistributedKeyValueStore/internal/server"
	"DistributedKeyValueStore/internal/store"
	"DistributedKeyValueStore/internal/replication"
)

func main() {
	// all are *string
    addr       := flag.String("addr", ":6379", "TCP listen address")
    aofPath    := flag.String("aof", "appendonly.aof", "AOF file path")
    aofSync    := flag.String("aof-sync", "everysecond", "AOF sync policy: always|everysecond|never")
    replicaOf  := flag.String("replicaof", "", "primary address for replica mode")
	replPort := flag.String("repl-port", ":6399", "replication listener port")
    flag.Parse()

	s := store.New()

    syncPolicy := persistence.SyncEverySecond
    switch strings.ToLower(*aofSync) {
    case "always":
        syncPolicy = persistence.SyncAlways
    case "never":
        syncPolicy = persistence.SyncNever
    }

	aof, err := persistence.NewAOF(*aofPath, syncPolicy)
	if err != nil {
		log.Fatalf("failed to open AOF: %v", err)
	}
	defer aof.Close()

	// Replay AOF to return state
	log.Println("replaying AOF...")
	if err := aof.Replay(func(args []string) error {
		return replayCommand(s, args)
	}); err != nil {
		log.Printf("AOF replay error: %v - continuing with partial state", err) // one bad AOF entry should not crash the entire server
	}
	log.Printf("store has %d keys after replay", s.Len())
	
	var primary *replication.Primary
    if *replicaOf == "" {
        primary = replication.NewPrimary()
        go primary.ListenForReplicas(*replPort)
    } else {
		go replication.StartReplica(*replicaOf, s)
	}

	srv := server.New(*addr, s, aof, primary)

	// Graceful shutdown on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("shutting down...")
		cancel()
	}()

	if err := srv.ListenAndServe(ctx); err != nil {
		log.Fatal(err)
	}

	if primary != nil {
		primary.Close() // closes all replica connections, terminates handleReplica goroutines
    }
}

// Applies a single deserialized command directly to the store.
func replayCommand(s *store.Store, args []string) error {
    if len(args) == 0 {
        return nil
    }
    switch strings.ToUpper(args[0]) {
    case "SET":
		if len(args) < 3 {
			return nil
		}

		key, value := args[1], args[2]	
		var ttl time.Duration
		for i := 3; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "EX":
				if i+1 < len(args) {
					secs, _ := strconv.ParseInt(args[i+1], 10, 64)
					ttl = time.Duration(secs) * time.Second
					i++
				}
			case "PX":
				if i+1 < len(args) {
					ms, _ := strconv.ParseInt(args[i+1], 10, 64)
					ttl = time.Duration(ms) * time.Millisecond
					i++
				}
			}
			// NX/XX don't need to be honored during replay
		}
		s.Set(key, value, ttl)
    case "DEL":
        s.Delete(args[1:]...)
    case "EXPIRE":
        if len(args) == 3 {
            secs, _ := strconv.ParseInt(args[2], 10, 64)
            s.Expire(args[1], time.Duration(secs)*time.Second)
        }
	}
	return nil
}
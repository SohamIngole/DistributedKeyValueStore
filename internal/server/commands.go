package server

import (
    "strconv"
    "strings"
    "time"

	"DistributedKeyValueStore/internal/resp"
)

func (s *Server) commandSet(w *resp.Writer, args []string) {
    if len(args) < 3 {
        w.WriteError("wrong number of arguments for 'set' command")
        return
    }
    key, value := args[1], args[2]
    var ttl time.Duration

    // Parse optional flags: EX seconds | PX milliseconds | NX | XX
    for i := 3; i < len(args); i++ {
        switch strings.ToUpper(args[i]) {
        case "EX":
            if i+1 >= len(args) {
                w.WriteError("syntax error")
                return
            }
            secs, err := strconv.ParseInt(args[i+1], 10, 64)
            if err != nil || secs <= 0 {
                w.WriteError("invalid expire time in 'set' command")
                return
            }
            ttl = time.Duration(secs) * time.Second
            i++
        case "PX":
            if i+1 >= len(args) {
                w.WriteError("syntax error")
                return
            }
            ms, err := strconv.ParseInt(args[i+1], 10, 64)
            if err != nil || ms <= 0 {
                w.WriteError("invalid expire time in 'set' command")
                return
            }
            ttl = time.Duration(ms) * time.Millisecond
            i++
        case "NX":
            // Only set if key does not exist
            if s.store.Exists(key) {
                w.WriteBulkString("", false)  // nil response
                return
            }
        case "XX":
            // Only set if key exists
            if !s.store.Exists(key) {
                w.WriteBulkString("", false)  // nil response
                return
            }
        }
    }

    s.store.Set(key, value, ttl)
    if s.aof != nil {
        s.aof.Append(args)
    }
    w.WriteSimpleString("OK")
}

func (s *Server) commandDel(w *resp.Writer, args []string) {
    if len(args) < 2 {
        w.WriteError("wrong number of arguments for 'del' command")
        return
    }
    keys := args[1:]
    deleted := s.store.Delete(keys...)
    if s.aof != nil {
        s.aof.Append(args)
    }
    w.WriteInteger(deleted)
}

func (s *Server) commandExists(w *resp.Writer, args []string) {
    if len(args) < 2 {
        w.WriteError("wrong number of arguments for 'exists' command")
        return
    }
    count := 0
    for _, key := range args[1:] {
        if s.store.Exists(key) {
            count++
        }
    }
    w.WriteInteger(count)
}

func (s *Server) commandExpire(w *resp.Writer, args []string) {
    if len(args) != 3 {
        w.WriteError("wrong number of arguments for 'expire' command")
        return
    }
    secs, err := strconv.ParseInt(args[2], 10, 64)
    if err != nil {
        w.WriteError("value is not an integer or out of range")
        return
    }
    ok := s.store.Expire(args[1], time.Duration(secs)*time.Second)
    if ok {
        w.WriteInteger(1)
    } else {
        w.WriteInteger(0)
    }
}

func (s *Server) commandTTL(w *resp.Writer, args []string) {
    if len(args) != 2 {
        w.WriteError("wrong number of arguments for 'ttl' command")
        return
    }
    d := s.store.TTL(args[1])
    switch d {
    case -1:
        w.WriteInteger(-1)  // no expiry
    case -2:
        w.WriteInteger(-2)  // key doesn't exist
    default:
        w.WriteInteger(int(d.Seconds()))
    }
}

func (s *Server) commandKeys(w *resp.Writer, args []string) {
    if len(args) != 2 {
        w.WriteError("wrong number of arguments for 'keys' command")
        return
    }
    keys := s.store.Keys(args[1])
    w.WriteArray(keys)
}

func (s *Server) commandPersist(w *resp.Writer, args []string) {
    if len(args) != 2 {
        w.WriteError("wrong number of arguments for 'persist' command")
        return
    }
    if s.store.Persist(args[1]) {
        w.WriteInteger(1)
    } else {
        w.WriteInteger(0)
    }
}
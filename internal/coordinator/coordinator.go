package coordinator

import (
    "bufio"
    "fmt"
    "log"
    "net"
    "strings"
    "sync"
    "strconv"
    "io"
    "context"
    "time"
    "errors"
    "syscall"

    "DistributedKeyValueStore/internal/cluster"
    "DistributedKeyValueStore/internal/resp"
)

type Coordinator struct {
    addr  string
    ring  *cluster.Ring
    pools sync.Map    // map[nodeAddr]*connPool
    connections sync.WaitGroup
}

func New(addr string) *Coordinator {
    return &Coordinator{addr: addr, ring: cluster.NewRing(150)}
}

func (c *Coordinator) AddNode(addr string) {
	c.ring.AddNode(addr) // places addr's 150 virtual nodes onto the ring
	pool := cluster.NewConnPool(addr, 10)
	c.pools.Store(addr, pool)
    log.Printf("coordinator: added node %s", addr)
}

func (c *Coordinator) ListenAndServe(ctx context.Context) error {
    ln, err := net.Listen("tcp", c.addr)
    if err != nil {
        return err
    }
    log.Printf("coordinator listening on %s", c.addr)

    go func() {
        <-ctx.Done()
        ln.Close()
    }()

    for {
        conn, err := ln.Accept()
        if err != nil {
            if ctx.Err() != nil {
                break   // intentional shutdown
            }
            log.Printf("coordinator: accept error: %v", err)
            continue
        }
        c.connections.Add(1)
        go c.handleClient(conn)
    }

    c.connections.Wait()
    log.Println("coordinator shutdown complete")
    return nil
}

func (c *Coordinator) handleClient(client net.Conn) {
	defer func() {
		client.Close()
		c.connections.Done()
	}()

	// Idle timeout: drop connections inactive for 5 minutes
	client.SetDeadline(time.Now().Add(5 * time.Minute))

	r := bufio.NewReader(client)
	w := resp.NewWriter(client)

	for {
		// Reset deadline on each command
		client.SetDeadline(time.Now().Add(5 * time.Minute))

		args, err := resp.ReadCommand(r)
		if err != nil {
			if !isConnectionClosed(err) {
				log.Printf("coordinator: read error from %s: %v", client.RemoteAddr(), err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PING":
			w.WriteSimpleString("PONG")
			continue
		case "DBSIZE":
			c.handleDBSize(w)
			continue
		}

		if len(args) < 2 {
			w.WriteError("wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
			continue
		}

		key := args[1]
		nodeAddr, ok := c.ring.GetNode(key)
		if !ok {
			w.WriteError("no nodes available")
			continue
		}

		if err := c.forward(w, nodeAddr, args); err != nil {
			w.WriteError(fmt.Sprintf("upstream error: %v", err))
		}
	}
}


func (c *Coordinator) forward(w *resp.Writer, nodeAddr string, args []string) error {
    val, ok := c.pools.Load(nodeAddr)
    if !ok {
        return fmt.Errorf("no pool for node %s", nodeAddr)
    }
    pool := val.(*cluster.ConnPool)

    conn, err := pool.Get()
    if err != nil {
        return fmt.Errorf("dial %s: %w", nodeAddr, err)
    }

    // Write the command to the node
    fmt.Fprintf(conn, "*%d\r\n", len(args))
    for _, a := range args {
        fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(a), a)
    }

    // Read and forward the response back to the client
    r := bufio.NewReader(conn)
    line, err := r.ReadString('\n')
    if err != nil {
        conn.Close()
        return err
    }

    // Write raw response back (transparent forwarding)
    fmt.Fprint(w.Underlying(), line)

    // For bulk strings and arrays, we need to read more
    if len(line) > 0 && (line[0] == '$' || line[0] == '*') {
        c.readAndForwardBody(r, w.Underlying(), line)
    }

    pool.Put(conn)
    return nil
}

func (c *Coordinator) readAndForwardBody(r *bufio.Reader, dst io.Writer, header string) error {
    if header[0] == '$' {
        // bulk string: read exactly n+2 bytes (data + \r\n)
        n, _ := strconv.Atoi(strings.TrimRight(header[1:], "\r\n"))
        if n < 0 {
            return nil  
        }
        buf := make([]byte, n+2)
        io.ReadFull(r, buf)
        fmt.Fprint(dst, string(buf))

    } else if header[0] == '*' {
        // array: read count bulk strings
        count, _ := strconv.Atoi(strings.TrimRight(header[1:], "\r\n"))
        for i := 0; i < count; i++ {
            line, _ := r.ReadString('\n')
            fmt.Fprint(dst, line)
            if len(line) > 0 && line[0] == '$' {
                c.readAndForwardBody(r, dst, line)  // recursive for nested
            }
        }
    }
    return nil
}

// Aggregates DBSIZE from all nodes
func (c *Coordinator) handleDBSize(w *resp.Writer) {
	nodes := c.ring.Nodes()
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0

	for _, nodeAddr := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			size := c.dbSizeFromNode(addr)
			mu.Lock()
			total += size
			mu.Unlock()
		}(nodeAddr)
	}

	wg.Wait()                  // Wait for ALL goroutines to finish, then write the sum to the client
	w.WriteInteger(total)      
}

func (c *Coordinator) dbSizeFromNode(nodeAddr string) int {
    val, ok := c.pools.Load(nodeAddr)
    if !ok {
        return 0
    }
    pool := val.(*cluster.ConnPool)

    conn, err := pool.Get()
    if err != nil {
        return 0
    }

    // Send DBSIZE as RESP
    fmt.Fprintf(conn, "*1\r\n$6\r\nDBSIZE\r\n")

    // Read the integer reply ":N\r\n"
    r := bufio.NewReader(conn)
    line, err := r.ReadString('\n')
    if err != nil {
        conn.Close()
        return 0
    }

    pool.Put(conn)

    // Parse ":N\r\n" -> N
    line = strings.TrimRight(line, "\r\n")
    if len(line) < 2 || line[0] != ':' {
        return 0
    }
    n, _ := strconv.Atoi(line[1:])
    return n
}

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
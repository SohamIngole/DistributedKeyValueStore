package resp

import (
	"fmt"
	"bufio"
	"io"
	"strconv"
	"strings"
)

// Running the fuzz test found these errors, so added an upper limit to fix them
const maxArrayLen = 1024 * 1024 // ~1 million elements
const maxBulkLen = 512 * 1024 * 1024 // 512MB

// Parses one RESP command from r, returning a slice of string arguments.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case '*':
		return readArray(r) 
	default:
		// Inline command: "PING\r\n" or "SET foo bar\r\n"
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}

		full := string(b) + strings.TrimRight(line, "\r\n")
		return strings.Fields(full), nil
	}
}

func readArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	if err != nil {
		return nil, fmt.Errorf("resp: invalid array count: %w", err)
	}
	if count < 0 {
		return nil, nil // null array
	}
	if count > maxArrayLen {
		return nil, fmt.Errorf("resp: array count %d exceeds maximum %d", count, maxArrayLen)
	}

	args := make([]string, 0, count) // 0 Length, count capacity
	for i := 0; i < count; i++ {
		s, err := readBulkString(r)
		if err != nil {
			return nil, err
		}
		args = append(args, s)
	}
	return args, nil
}

func readBulkString(r *bufio.Reader) (string, error) {
	b, err := r.ReadByte()
    if err != nil {
        return "", err
    }
    if b != '$' {
        return "", fmt.Errorf("resp: expected '$', got '%c'", b)
    }

    line, err := r.ReadString('\n')
    if err != nil {
        return "", err
    }
	n, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	if err != nil {
		return "", fmt.Errorf("resp: invalid bulk string length: %w", err)
	}
	if n < 0 {
		return "", nil // null bulk string
	}
	if n > maxBulkLen {
		return "", fmt.Errorf("resp: bulk string length %d exceeds maximum %d", n, maxBulkLen)
	}

	// Read exactly n bytes + CRLF
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("resp: reading bulk string data: %w", err)
	}
	return string(buf[:n]), nil
}
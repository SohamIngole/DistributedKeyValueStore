package resp_test
import (
	"testing"
	"bufio"
	"bytes"

	"DistributedKeyValueStore/internal/resp"
)

func FuzzReadCommand(f *testing.F) {
    // Seed corpus with valid inputs
    f.Add([]byte("*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n"))
    f.Add([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))

    f.Fuzz(func(t *testing.T, data []byte) {
        r := bufio.NewReader(bytes.NewReader(data))
        // Must not panic — either return args or an error
        // If it panics, the fuzzer will catch it
        resp.ReadCommand(r)
    })
}
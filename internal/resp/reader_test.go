package resp_test

import (
	"bufio"
	"reflect"
	"strings"
	"testing"

	"DistributedKeyValueStore/internal/resp"
)
func TestReadCommand(t *testing.T) {
    tests := []struct {
        name  string
        input string
        want  []string
    }{
        {"SET command",    "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", []string{"SET", "foo", "bar"}},
        {"GET command",    "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",               []string{"GET", "foo"}},
        {"PING inline",    "PING\r\n",                                          []string{"PING"}},
        {"empty value",    "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$0\r\n\r\n",    []string{"SET", "foo", ""}},
        {"unicode value",  "*2\r\n$3\r\nGET\r\n$6\r\n你好\r\n",              []string{"GET", "你好"}},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            r := bufio.NewReader(strings.NewReader(tt.input))
            got, err := resp.ReadCommand(r)
            if err != nil {
                t.Fatalf("unexpected error: %v", err)
            }
            if !reflect.DeepEqual(got, tt.want) {
                t.Errorf("got %v, want %v", got, tt.want)
            }
        })
    }
}
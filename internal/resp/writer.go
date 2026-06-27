package resp

import(
	"fmt"
	"io"
	"strconv"
)

// Writer wraps an io.Writer with RESP-aware write methods
type Writer struct {
	w io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// WriteSimpleString writes "+OK\r\n"
func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.w, "%s\r\n", s)
	return err
}

// WriteError writes "-ERR message\r\n"
func (w *Writer) WriteError(msg string) error {
    _, err := fmt.Fprintf(w.w, "-ERR %s\r\n", msg)
    return err
}

// WriteErrorType writes "-WRONGTYPE ...\r\n" (for type mismatch errors)
func (w *Writer) WriteErrorType(errType, msg string) error {
    _, err := fmt.Fprintf(w.w, "-%s %s\r\n", errType, msg)
    return err
}

// WriteBulkString writes "$N\r\nvalue\r\n" or "$-1\r\n" for nil
func (w *Writer) WriteBulkString(s string, exists bool) error {
    if !exists {
        _, err := fmt.Fprint(w.w, "$-1\r\n")
        return err
    }
    _, err := fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(s), s)
    return err
}

// WriteInteger writes ":N\r\n"
func (w *Writer) WriteInteger(n int) error {
    _, err := fmt.Fprintf(w.w, ":%s\r\n", strconv.Itoa(n))
    return err
}

// WriteArray writes "*N\r\n" followed by N bulk strings
func (w *Writer) WriteArray(items []string) error {
    if items == nil {
        _, err := fmt.Fprint(w.w, "*-1\r\n")
        return err
    }
    if _, err := fmt.Fprintf(w.w, "*%d\r\n", len(items)); err != nil {
        return err
    }
    for _, item := range items {
        if err := w.WriteBulkString(item, true); err != nil {
            return err
        }
    }
    return nil
}
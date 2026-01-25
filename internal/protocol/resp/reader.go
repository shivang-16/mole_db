package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// Reader implements a minimal subset of RESP suitable for Mole's command loop.
//
// Supported request forms:
//   - RESP Arrays of Bulk Strings (used by typical Redis clients):
//     *<n>\r\n$<len>\r\n<data>\r\n...
//   - Inline commands (handy for nc during development):
//     PING\r\n
//     SET key value\r\n
type Reader struct {
	r *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(rd)}
}

// ReadCommand returns a command as [[]byte("CMD"), []byte("arg1"), ...].
// All arguments are kept as raw bytes for binary-safety.
func (rr *Reader) ReadCommand() ([][]byte, error) {
	b, err := rr.r.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0] == '*' {
		return rr.readArrayOfBulkStrings()
	}
	return rr.readInline()
}

func (rr *Reader) readArrayOfBulkStrings() ([][]byte, error) {
	line, err := rr.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("%w: expected array", ErrProtocol)
	}
	n, err := parseInt(line[1:])
	if err != nil || n < 0 {
		return nil, fmt.Errorf("%w: bad array length", ErrProtocol)
	}

	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		h, err := rr.readLine()
		if err != nil {
			return nil, err
		}
		if len(h) == 0 || h[0] != '$' {
			return nil, fmt.Errorf("%w: expected bulk string", ErrProtocol)
		}

		l, err := parseInt(h[1:])
		if err != nil || l < -1 {
			return nil, fmt.Errorf("%w: bad bulk length", ErrProtocol)
		}
		if l == -1 {
			out = append(out, nil)
			continue
		}

		buf := make([]byte, l+2) // data + CRLF
		if _, err := io.ReadFull(rr.r, buf); err != nil {
			return nil, err
		}
		if buf[l] != '\r' || buf[l+1] != '\n' {
			return nil, fmt.Errorf("%w: missing CRLF after bulk string", ErrProtocol)
		}

		// Keep as raw bytes for binary-safety.
		out = append(out, buf[:l])
	}
	return out, nil
}

func (rr *Reader) readInline() ([][]byte, error) {
	line, err := rr.readLine()
	if err != nil {
		return nil, err
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, fmt.Errorf("%w: empty command", ErrProtocol)
	}
	parts := bytes.Fields(line)
	out := make([][]byte, 0, len(parts))
	for _, p := range parts {
		out = append(out, p)
	}
	return out, nil
}

func (rr *Reader) readLine() ([]byte, error) {
	line, err := rr.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("%w: expected CRLF", ErrProtocol)
	}
	return bytes.TrimSuffix(line, []byte("\r\n")), nil
}

func parseInt(b []byte) (int, error) {
	return strconv.Atoi(string(b))
}

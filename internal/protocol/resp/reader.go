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
//   - RESP Arrays of Bulk Strings (used by standard RESP clients):
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

		// Create independent copy to avoid buffer reuse issues.
		copied := make([]byte, l)
		copy(copied, buf[:l])
		out = append(out, copied)
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
	return tokenizeInline(line)
}

// tokenizeInline parses an inline command respecting quoted strings.
func tokenizeInline(line []byte) ([][]byte, error) {
	var out [][]byte
	i := 0
	n := len(line)

	for i < n {
		// Skip whitespace between tokens.
		for i < n && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= n {
			break
		}

		var token []byte
		var err error

		switch line[i] {
		case '"':
			token, i, err = parseDoubleQuoted(line, i)
		case '\'':
			token, i, err = parseSingleQuoted(line, i)
		default:
			token, i = parseUnquoted(line, i)
		}

		if err != nil {
			return nil, err
		}
		out = append(out, token)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("%w: empty command", ErrProtocol)
	}
	return out, nil
}

// parseDoubleQuoted parses a double-quoted string with escape sequences.
func parseDoubleQuoted(line []byte, start int) ([]byte, int, error) {
	n := len(line)
	i := start + 1 // skip opening quote
	var buf []byte

	for i < n {
		ch := line[i]
		if ch == '"' {
			return buf, i + 1, nil
		}
		if ch == '\\' && i+1 < n {
			i++
			switch line[i] {
			case 'n':
				buf = append(buf, '\n')
			case 'r':
				buf = append(buf, '\r')
			case 't':
				buf = append(buf, '\t')
			case '\\':
				buf = append(buf, '\\')
			case '"':
				buf = append(buf, '"')
			case 'x':
				// Hex escape: \xNN
				if i+2 < n {
					hex := line[i+1 : i+3]
					if val, err := parseHexByte(hex); err == nil {
						buf = append(buf, val)
						i += 2
					} else {
						buf = append(buf, '\\', 'x')
					}
				} else {
					buf = append(buf, '\\', 'x')
				}
			default:
				// Unknown escape, keep as-is.
				buf = append(buf, line[i])
			}
		} else {
			buf = append(buf, ch)
		}
		i++
	}
	return nil, start, fmt.Errorf("%w: unclosed double quote", ErrProtocol)
}

// parseSingleQuoted parses a single-quoted string (no escape processing).
func parseSingleQuoted(line []byte, start int) ([]byte, int, error) {
	n := len(line)
	i := start + 1 // skip opening quote
	var buf []byte

	for i < n {
		ch := line[i]
		if ch == '\'' {
			return buf, i + 1, nil
		}
		buf = append(buf, ch)
		i++
	}
	return nil, start, fmt.Errorf("%w: unclosed single quote", ErrProtocol)
}

// parseUnquoted parses an unquoted token (until whitespace).
func parseUnquoted(line []byte, start int) ([]byte, int) {
	n := len(line)
	i := start
	for i < n && line[i] != ' ' && line[i] != '\t' {
		i++
	}
	token := make([]byte, i-start)
	copy(token, line[start:i])
	return token, i
}

// parseHexByte parses a 2-character hex string into a byte.
func parseHexByte(hex []byte) (byte, error) {
	if len(hex) != 2 {
		return 0, fmt.Errorf("invalid hex")
	}
	val, err := strconv.ParseUint(string(hex), 16, 8)
	if err != nil {
		return 0, err
	}
	return byte(val), nil
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

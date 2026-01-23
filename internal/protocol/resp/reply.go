package resp

// Reply represents a server-to-client RESP response.
//
// Keeping replies in the protocol package lets command logic return protocol
// responses without knowing about networking details.
type Reply interface {
	WriteTo(w *Writer) error
}

type SimpleString string

func (s SimpleString) WriteTo(w *Writer) error { return w.WriteSimpleString(string(s)) }

type Error string

func (e Error) WriteTo(w *Writer) error { return w.WriteError(string(e)) }

type Integer int64

func (i Integer) WriteTo(w *Writer) error { return w.WriteInteger(int64(i)) }

type BulkString []byte

func (b BulkString) WriteTo(w *Writer) error { return w.WriteBulkString([]byte(b)) }

// NullBulkString represents "$-1\r\n".
type NullBulkString struct{}

func (n NullBulkString) WriteTo(w *Writer) error { return w.WriteNullBulkString() }

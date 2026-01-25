package resp

import (
	"bufio"
	"io"
	"strconv"
)

type Writer struct {
	w *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

func (wr *Writer) Flush() error { return wr.w.Flush() }

func (wr *Writer) WriteSimpleString(s string) error {
	_, err := wr.w.WriteString("+" + s + "\r\n")
	return err
}

func (wr *Writer) WriteError(msg string) error {
	_, err := wr.w.WriteString("-" + msg + "\r\n")
	return err
}

func (wr *Writer) WriteInteger(i int64) error {
	_, err := wr.w.WriteString(":" + strconv.FormatInt(i, 10) + "\r\n")
	return err
}

func (wr *Writer) WriteNullBulkString() error {
	_, err := wr.w.WriteString("$-1\r\n")
	return err
}

func (wr *Writer) WriteBulkString(b []byte) error {
	if b == nil {
		return wr.WriteNullBulkString()
	}
	if _, err := wr.w.WriteString("$" + strconv.Itoa(len(b)) + "\r\n"); err != nil {
		return err
	}
	if _, err := wr.w.Write(b); err != nil {
		return err
	}
	_, err := wr.w.WriteString("\r\n")
	return err
}

// WriteArray writes a RESP array of bulk strings.
func (wr *Writer) WriteArray(args [][]byte) error {
	if _, err := wr.w.WriteString("*" + strconv.Itoa(len(args)) + "\r\n"); err != nil {
		return err
	}
	for _, arg := range args {
		if err := wr.WriteBulkString(arg); err != nil {
			return err
		}
	}
	return nil
}

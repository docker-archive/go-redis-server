package redis

import (
	"bytes"
	"errors"
	"io"
	"strconv"
)

type StatusReply struct {
	code string
}

type ReplyWriter interface {
	WriteTo(w io.Writer) (int, error)
}

func (r *StatusReply) WriteTo(w io.Writer) (int, error) {
	Debugf("Status")
	return w.Write([]byte("+" + r.code + "\r\n"))
}

type ErrorReply struct {
	code    string
	message string
}

func (r *ErrorReply) WriteTo(w io.Writer) (int, error) {
	return w.Write([]byte("-" + r.code + " " + r.message + "\r\n"))
}

type IntegerReply struct {
	number int
}

func (r *IntegerReply) WriteTo(w io.Writer) (int, error) {
	return w.Write([]byte(":" + strconv.Itoa(r.number) + "\r\n"))
}

type BulkReply struct {
	value []byte
}

func writeBytes(value []byte, w io.Writer) (int, error) {
	//it's a NullBulkReply
	if value == nil {
		return w.Write([]byte("$-1\r\n"))
	}

	wrote, err := w.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n"))
	if err != nil {
		return wrote, err
	}
	wroteBytes, err := w.Write(value)
	if err != nil {
		return wrote + wroteBytes, err
	}
	wroteCrLf, err := w.Write([]byte("\r\n"))
	return wrote + wroteBytes + wroteCrLf, err
}

func (r *BulkReply) WriteTo(w io.Writer) (int, error) {
	return writeBytes(r.value, w)
}

//for nil reply in multi bulk just set []byte as nil
type MultiBulkReply struct {
	values [][]byte
}

func MultiBulkFromMap(m *map[string][]byte) *MultiBulkReply {
	values := make([][]byte, len(*m)*2)
	i := 0
	for key, val := range *m {
		values[i] = []byte(key)
		values[i+1] = val
		i += 2
	}
	return &MultiBulkReply{values: values}
}

func writeMultiBytes(values [][]byte, w io.Writer) (int, error) {
	if values == nil {
		return 0, errors.New("Nil in multi bulk replies are not ok")
	}
	wrote, err := w.Write([]byte("*" + strconv.Itoa(len(values)) + "\r\n"))
	if err != nil {
		return wrote, err
	}
	for _, v := range values {
		wroteBytes, err := writeBytes(v, w)
		if err != nil {
			return wrote + wroteBytes, err
		}
		wrote += wroteBytes
	}
	return wrote, err
}

func (r *MultiBulkReply) WriteTo(w io.Writer) (int, error) {
	return writeMultiBytes(r.values, w)
}

func NewError(message string) *ErrorReply {
	return &ErrorReply{code: "ERROR", message: message}
}

func methodNotSupported() ReplyWriter {
	return NewError("Method is not supported")
}

func ReplyToString(r ReplyWriter) (string, error) {
	var b bytes.Buffer
	_, err := r.WriteTo(&b)
	if err != nil {
		return "ERROR!", err
	}
	return b.String(), nil
}

type ChannelWriter struct {
	FirstReply [][]byte
	Channel    chan [][]byte
}

func (c *ChannelWriter) WriteTo(w io.Writer) (int, error) {
	totalBytes, err := writeMultiBytes(c.FirstReply, w)
	if err != nil {
		return totalBytes, err
	}

	for {
		select {
		case reply := <-c.Channel:
			if reply == nil {
				return totalBytes, nil
			} else {
				wroteBytes, err := writeMultiBytes(reply, w)
				// FIXME: obvious overflow here,
				// Just ignore? Who cares?
				totalBytes += wroteBytes
				if err != nil {
					return totalBytes, err
				}
			}
		}
	}
	return totalBytes, nil
}

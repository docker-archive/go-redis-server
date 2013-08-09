package redis

import (
	"bytes"
	"errors"
	"io"
	"strconv"
)

type ReplyWriter io.WriterTo

type StatusReply struct {
	code string
}

func (r *StatusReply) WriteTo(w io.Writer) (int64, error) {
	Debugf("Status")
	n, err := w.Write([]byte("+" + r.code + "\r\n"))
	return int64(n), err
}

type IntegerReply struct {
	number int
}

func (r *IntegerReply) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte(":" + strconv.Itoa(r.number) + "\r\n"))
	return int64(n), err
}

type BulkReply struct {
	value []byte
}

func writeBytes(value []byte, w io.Writer) (int64, error) {
	//it's a NullBulkReply
	if value == nil {
		n, err := w.Write([]byte("$-1\r\n"))
		return int64(n), err
	}

	wrote, err := w.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n"))
	if err != nil {
		return int64(wrote), err
	}
	wroteBytes, err := w.Write(value)
	if err != nil {
		return int64(wrote + wroteBytes), err
	}
	wroteCrLf, err := w.Write([]byte("\r\n"))
	return int64(wrote + wroteBytes + wroteCrLf), err
}

func (r *BulkReply) WriteTo(w io.Writer) (int64, error) {
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

func writeMultiBytes(values [][]byte, w io.Writer) (int64, error) {
	if values == nil {
		return 0, errors.New("Nil in multi bulk replies are not ok")
	}
	wrote, err := w.Write([]byte("*" + strconv.Itoa(len(values)) + "\r\n"))
	if err != nil {
		return int64(wrote), err
	}
	wrote64 := int64(wrote)
	for _, v := range values {
		wroteBytes, err := writeBytes(v, w)
		if err != nil {
			return wrote64 + wroteBytes, err
		}
		wrote64 += wroteBytes
	}
	return wrote64, err
}

func (r *MultiBulkReply) WriteTo(w io.Writer) (int64, error) {
	return writeMultiBytes(r.values, w)
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

func (c *ChannelWriter) WriteTo(w io.Writer) (int64, error) {
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

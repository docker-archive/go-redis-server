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

func (r *MultiBulkReply) WriteTo(w io.Writer) (int, error) {
	if r.values == nil {
		return 0, errors.New("Nil in multi bulk replies are not ok")
	}
	wrote, err := w.Write([]byte("*" + strconv.Itoa(len(r.values)) + "\r\n"))
	if err != nil {
		return wrote, err
	}
	for _, v := range r.values {
		wroteBytes, err := writeBytes(v, w)
		if err != nil {
			return wrote + wroteBytes, err
		}
		wrote += wroteBytes
	}
	return wrote, err
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

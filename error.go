package redis

import (
	"io"
)

type ErrorReply struct {
	code    string
	message string
}

func (er *ErrorReply) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte("-" + er.code + " " + er.message + "\r\n"))
	return int64(n), err
}

func (er *ErrorReply) Error() string {
	return "-" + er.code + " " + er.message + "\r\n"
}

func NewError(message string) *ErrorReply {
	return &ErrorReply{code: "ERROR", message: message}
}

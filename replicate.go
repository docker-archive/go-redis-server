package livedb

import (
	"bufio"
	"io"
	"errors"
	"fmt"
)

type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader{
	return &Reader{r: bufio.NewReader(r)}
}


func (r *Reader) Read() (Command, error) {
	var words []string
	var nArgs int
	if _, err := r.r.Peek(1); err == io.EOF {
		return nil, io.EOF
	}
	if _, err := fmt.Fscanf(r.r, "*%d\r\n", &nArgs); err != nil {
		return nil, err
	}
	for i:=0; i<nArgs; i+=1 {
		var argSize int
		if _, err := fmt.Fscanf(r.r, "$%d\r\n", &argSize); err != nil {
			return nil, err
		}
		argData := make([]byte, argSize + 2)
		if _, err := r.r.Read(argData); err != nil {
			return nil, err
		}
		if string(argData[argSize:]) != "\r\n" {
			return nil, errors.New("Protocol error")
		}
		words = append(words, string(argData[:argSize]))
	}
	if len(words) == 0 {
		return nil, errors.New("Empty command")
	}
	return words, nil
}

type ReplicationWriter struct {
	w io.Writer
}

func (w *ReplicationWriter) Apply(words... string) (interface{}, error) {
	if len(words) == 0 {
		return nil, errors.New("Empty command")
	}
	var nSent int
	if n, err := fmt.Fprintf(w.w, "*%d\r\n", len(words)); err != nil {
		return nSent, err
	} else {
		nSent += n
	}
	for i:=0; i<len(words); i+=1 {
		if n, err := fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(words[i]), words[i]); err != nil {
			return nSent, err
		} else {
			nSent += n
		}
	}
	return nSent, nil
}


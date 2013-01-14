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


func (r *Reader) Read() (string, string, []string, error) {
	var words []string
	var nArgs int
	if _, err := r.r.Peek(1); err == io.EOF {
		return "", "", nil, io.EOF
	}
	if _, err := fmt.Fscanf(r.r, "*%d\r\n", &nArgs); err != nil {
		return "", "", nil, err
	}
	for i:=0; i<nArgs; i+=1 {
		var argSize int
		if _, err := fmt.Fscanf(r.r, "$%d\r\n", &argSize); err != nil {
			return "", "", nil, err
		}
		argData := make([]byte, argSize + 2)
		if _, err := r.r.Read(argData); err != nil {
			return "", "", nil, err
		}
		if string(argData[argSize:]) != "\r\n" {
			return "", "", nil, errors.New("Protocol error")
		}
		words = append(words, string(argData[:argSize]))
	}
	if len(words) >= 2 {
		return words[0], words[1], words[2:], nil
	} else if len(words) == 1 {
		return words[0], "", []string{}, nil
	}
	return "", "", []string{}, nil
}

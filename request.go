package redis

import (
	"io"
	"strconv"
)

type Request struct {
	Name       string
	Args       [][]byte
	Host       string
	ClientChan chan struct{}
	Body       io.ReadCloser
}

func (r *Request) HasArgument(index int) bool {
	return len(r.Args) >= index+1
}

func (r *Request) ExpectArgument(index int) ReplyWriter {
	if !r.HasArgument(index) {
		return ErrNotEnoughArgs
	}
	return nil
}

func (r *Request) GetString(index int) (string, ReplyWriter) {
	if reply := r.ExpectArgument(index); reply != nil {
		return "", reply
	}
	return string(r.Args[index]), nil
}

func (r *Request) GetInteger(index int) (int, ReplyWriter) {
	if reply := r.ExpectArgument(index); reply != nil {
		return -1, reply
	}
	i, err := strconv.Atoi(string(r.Args[index]))
	if err != nil {
		return -1, ErrExpectInteger
	}
	return i, nil
}

func (r *Request) GetPositiveInteger(index int) (int, ReplyWriter) {
	i, reply := r.GetInteger(index)
	if reply != nil {
		return -1, reply
	}
	if i < 0 {
		return -1, ErrExpectPositivInteger
	}
	return i, nil
}

func (r *Request) GetStringSlice(index int) ([]string, ReplyWriter) {
	if reply := r.ExpectArgument(index); reply != nil {
		return nil, reply
	}
	var ret []string
	for _, elem := range r.Args[index:] {
		ret = append(ret, string(elem))
	}
	return ret, nil
}

func (r *Request) GetMap(index int) (map[string][]byte, ReplyWriter) {
	count := len(r.Args) - index
	if count <= 0 {
		return nil, ErrExpectMorePair
	}
	if count%2 != 0 {
		return nil, ErrExpectEvenPair
	}
	values := make(map[string][]byte)
	for i := index; i < len(r.Args); i += 2 {
		key, reply := r.GetString(i)
		if reply != nil {
			return nil, reply
		}
		values[key] = r.Args[i+1]
	}
	return values, nil
}

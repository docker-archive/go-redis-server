package redis

import (
	"strconv"
)

type Request struct {
	name string
	args [][]byte
}

func (request *Request) ExpectArgument(index int) ReplyWriter {
	if len(request.args) < index+1 {
		return NewError("Not enough arguments")
	}
	return nil
}

func (request *Request) GetString(index int) (string, ReplyWriter) {
	if reply := request.ExpectArgument(index); reply != nil {
		return "", reply
	}
	return string(request.args[index]), nil
}

func (request *Request) GetInteger(index int) (int, ReplyWriter) {
	if reply := request.ExpectArgument(index); reply != nil {
		return -1, reply
	}
	i, err := strconv.Atoi(string(request.args[index]))
	if err != nil {
		return -1, NewError("Expected integer")
	}
	return i, nil
}

func (request *Request) GetPositiveInteger(index int) (int, ReplyWriter) {
	i, reply := request.GetInteger(index)
	if reply != nil {
		return -1, reply
	}
	if i < 0 {
		return -1, NewError("Expected positive integer")
	}
	return i, nil
}

func (request *Request) GetMap(index int) (*map[string][]byte, ReplyWriter) {
	count := len(request.args) - index
	if count <= 0 {
		return nil, NewError("Expected at least one key val pair")
	}
	if count%2 != 0 {
		return nil, NewError("Got uneven number of key val pairs")
	}
	values := make(map[string][]byte)
	for i := index; i < len(request.args); i += 2 {
		key, reply := request.GetString(i)
		if reply != nil {
			return nil, reply
		}
		values[key] = request.args[i+1]
	}
	return &values, nil
}

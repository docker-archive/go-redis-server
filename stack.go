package redis

import (
	"sync"
)

type Stack struct {
	sync.Mutex
	stack [][]byte
	Chan  chan *Stack
	Key   []byte
}

func (s *Stack) PopBack() []byte {
	s.Lock()
	defer s.Unlock()

	if s.stack == nil || len(s.stack) == 0 {
		return nil
	}

	var ret []byte
	if len(s.stack)-1 == 0 {
		ret, s.stack = s.stack[0], [][]byte{}
	} else {
		ret, s.stack = s.stack[len(s.stack)-1], s.stack[:len(s.stack)-1]
	}
	return ret
}

func (s *Stack) PushBash(val []byte) {
	s.Lock()
	defer s.Unlock()

	if s.stack == nil {
		s.stack = [][]byte{}
	}

	s.stack = append(s.stack, val)
	go func() {
		if s.Chan != nil {
			s.Chan <- s
		}
	}()
}

func (s *Stack) PopFront() []byte {
	s.Lock()
	defer s.Unlock()

	if s.stack == nil || len(s.stack) == 0 {
		return nil
	}

	var ret []byte
	if len(s.stack)-1 == 0 {
		ret, s.stack = s.stack[0], [][]byte{}
	} else {
		ret, s.stack = s.stack[0], s.stack[1:]
	}
	return ret
}

func (s *Stack) PushFront(val []byte) {
	s.Lock()
	defer s.Unlock()

	if s.stack == nil {
		s.stack = [][]byte{}
	}

	s.stack = append([][]byte{val}, s.stack...)
	go func() {
		if s.Chan != nil {
			s.Chan <- s
		}
	}()
}

func (s *Stack) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.stack)
}

func NewStack(key []byte) *Stack {
	return &Stack{
		stack: [][]byte{},
		Chan:  make(chan *Stack),
		Key:   key,
	}
}

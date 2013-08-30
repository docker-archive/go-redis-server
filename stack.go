package redis

import (
	"sync"
)

type Stack struct {
	sync.Mutex
	Key   string
	stack [][]byte
	Chan  chan *Stack
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

func (s *Stack) PushBack(val []byte) {
	s.Lock()
	defer s.Unlock()

	if s.stack == nil {
		s.stack = [][]byte{}
	}

	go func() {
		if s.Chan != nil {
			s.Chan <- s
		}
	}()
	s.stack = append(s.stack, val)
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

// GetIndex return the element at the requested index.
// If no element correspond, return nil.
func (s *Stack) GetIndex(index int) []byte {
	s.Lock()
	defer s.Unlock()

	if index < 0 {
		if len(s.stack)+index >= 0 {
			return s.stack[len(s.stack)+index]
		}
		return nil
	}
	if len(s.stack) > index {
		return s.stack[index]
	}
	return nil
}

func (s *Stack) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.stack)
}

func NewStack(key string) *Stack {
	return &Stack{
		stack: [][]byte{},
		Chan:  make(chan *Stack),
		Key:   key,
	}
}

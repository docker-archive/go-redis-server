package redis

import (
	"sync"
)

type BrStack struct {
	sync.Mutex
	stack [][]byte
	Chan  chan *BrStack
	Key   []byte
}

func (s *BrStack) Pop() []byte {
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

func (s *BrStack) Push(val []byte) {
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

func (s *BrStack) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.stack)
}

func NewBrStack(key []byte) *BrStack {
	return &BrStack{
		stack: [][]byte{},
		Chan:  make(chan *BrStack),
		Key:   key,
	}
}

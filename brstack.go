package redis

import (
	"sync"
)

type brStack struct {
	sync.Mutex
	stack [][]byte
	c     chan *brStack
	key   []byte
}

func (s *brStack) pop() []byte {
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

func (s *brStack) push(val []byte) {
	s.Lock()
	defer s.Unlock()

	if s.stack == nil {
		s.stack = [][]byte{}
	}

	s.stack = append(s.stack, val)
	go func() {
		if s.c != nil {
			s.c <- s
		}
	}()
}

func NewBrStack(key []byte) *brStack {
	return &brStack{
		stack: [][]byte{},
		c:     make(chan *brStack),
		key:   key,
	}
}

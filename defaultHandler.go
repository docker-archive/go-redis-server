package redis

import (
	"fmt"
	"reflect"
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
	s.stack = append(s.stack, val)
	go func() { s.c <- s }()
}

func NewBrStack(key []byte) *brStack {
	return &brStack{
		stack: [][]byte{},
		c:     make(chan *brStack),
		key:   key,
	}
}

type DefaultHandler struct {
	values  map[string][]byte
	hvalues map[string]map[string][]byte
	sub     map[string][]*ChannelWriter
	brstack map[string]*brStack
}

func (h *DefaultHandler) RPUSH(key string, values ...[]byte) (int, error) {
	if h.brstack == nil {
		h.brstack = make(map[string]*brStack)
	}
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewBrStack([]byte(key))
	}
	for _, value := range values {
		h.brstack[key].push(value)
	}
	return len(h.brstack[key].stack), nil
}

func (h *DefaultHandler) BRPOP(keys ...[]byte) ([][]byte, error) {
	if h.brstack == nil {
		h.brstack = make(map[string]*brStack)
	}

	selectCases := []reflect.SelectCase{}
	for _, k := range keys {
		key := string(k)
		if _, exists := h.brstack[key]; !exists {
			h.brstack[key] = NewBrStack(k)
		}
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(h.brstack[key].c),
		})
	}
	_, recv, _ := reflect.Select(selectCases)
	s, ok := recv.Interface().(*brStack)
	if !ok {
		return nil, fmt.Errorf("Impossible to retrieve data. Wrong type.")
	}
	return [][]byte{s.key, s.pop()}, nil
}

func (h *DefaultHandler) HGET(key, subkey string) ([]byte, error) {
	if h.hvalues != nil {
		if v, exists := h.hvalues[key]; exists {
			if v, exists := v[subkey]; exists {
				return v, nil
			}
		}
	}
	return nil, nil
}

func (h *DefaultHandler) HSET(key, subkey string, value []byte) error {
	if h.hvalues == nil {
		h.hvalues = make(map[string]map[string][]byte)
	}
	if _, exists := h.hvalues[key]; !exists {
		h.hvalues[key] = make(map[string][]byte)
	}
	h.hvalues[key][subkey] = value

	return nil
}

func (h *DefaultHandler) GET(key string) ([]byte, error) {
	if h.values == nil {
		h.values = make(map[string][]byte)
	}
	v, exists := h.values[key]
	if !exists {
		Debugf("The requested key [%s] does not exist", key)
		return nil, nil
	}
	Debugf("Getting key [%s] (%s)", key, v)
	return v, nil
}

func (h *DefaultHandler) SET(key string, value []byte) error {
	if h.values == nil {
		h.values = make(map[string][]byte)
	}
	Debugf("Setting key [%s] (%s)", key, value)
	h.values[key] = value
	return nil
}

func (h *DefaultHandler) SUBSCRIBE(channels ...[]byte) (*MultiChannelWriter, error) {
	ret := &MultiChannelWriter{Chans: make([]*ChannelWriter, 0, len(channels))}
	for _, key := range channels {
		Debugf("SUBSCRIBE on %s\n", key)
		cw := &ChannelWriter{
			FirstReply: []interface{}{
				"subscribe",
				key,
				1,
			},
			Channel: make(chan []interface{}),
		}
		if h.sub[string(key)] == nil {
			h.sub[string(key)] = []*ChannelWriter{cw}
		} else {
			h.sub[string(key)] = append(h.sub[string(key)], cw)
		}
		ret.Chans = append(ret.Chans, cw)
	}
	return ret, nil
}

func (h *DefaultHandler) PUBLISH(key string, value []byte) (int, error) {
	//	Debugf("Publishing %s on %s\n", value, key)
	v, exists := h.sub[key]
	if !exists {
		return 0, nil
	}
	i := 0
	for _, c := range v {
		select {
		case c.Channel <- []interface{}{
			"message",
			key,
			value,
		}:
			i++
		default:
		}
	}
	return i, nil
}

func (h *DefaultHandler) MONITOR() (*MonitorReply, error) {
	return &MonitorReply{}, nil
}

func NewDefaultHandler() *DefaultHandler {
	return &DefaultHandler{
		values:  make(map[string][]byte),
		sub:     make(map[string][]*ChannelWriter),
		brstack: make(map[string]*brStack),
	}
}

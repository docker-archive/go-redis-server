package redis

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type (
	HashValue   map[string][]byte
	HashHash    map[string]HashValue
	HashSub     map[string][]*ChannelWriter
	HashBrStack map[string]*Stack
)

type Database struct {
	children map[int]*Database
	parent   *Database

	values  HashValue
	hvalues HashHash
	brstack HashBrStack

	sub HashSub
}

func NewDatabase(parent *Database) *Database {
	db := &Database{
		values:   make(HashValue),
		sub:      make(HashSub),
		brstack:  make(HashBrStack),
		children: map[int]*Database{},
		parent:   parent,
	}
	db.children[0] = db
	return db
}

type DefaultHandler struct {
	*Database
	currentDb int
	dbs       map[int]*Database
}

func (h *DefaultHandler) Rpush(key string, value []byte, values ...[]byte) (int, error) {
	values = append([][]byte{value}, values...)
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}
	for _, value := range values {
		h.brstack[key].PushBack(value)
	}
	return h.brstack[key].Len(), nil
}

func (h *DefaultHandler) Brpop(key string, keys ...string) (data [][]byte, err error) {
	keys = append([]string{key}, keys...)
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}

	if len(keys) == 0 {
		return nil, ErrParseTimeout
	}

	timeout, err := strconv.Atoi(keys[len(keys)-1])
	if err != nil {
		return nil, ErrParseTimeout
	}
	keys = keys[:len(keys)-1]

	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(time.Duration(timeout) * time.Second)
	} else {
		timeoutChan = make(chan time.Time)
	}

	finishedChan := make(chan struct{})
	go func() {
		defer close(finishedChan)
		selectCases := []reflect.SelectCase{}
		for _, k := range keys {
			key := string(k)
			if _, exists := h.brstack[key]; !exists {
				h.brstack[key] = NewStack(k)
			}
			selectCases = append(selectCases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(h.brstack[key].Chan),
			})
		}
		_, recv, _ := reflect.Select(selectCases)
		s, ok := recv.Interface().(*Stack)
		if !ok {
			err = fmt.Errorf("Impossible to retrieve data. Wrong type.")
			return
		}
		data = [][]byte{[]byte(s.Key), s.PopBack()}
	}()

	select {
	case <-finishedChan:
		return data, err
	case <-timeoutChan:
		return nil, nil
	}
	return nil, nil
}

func (h *DefaultHandler) Lrange(key string, start, stop int) ([][]byte, error) {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}

	if start < 0 {
		if start = h.brstack[key].Len() + start; start < 0 {
			start = 0
		}
	}

	var ret [][]byte
	for i := start; i <= stop; i++ {
		if val := h.brstack[key].GetIndex(i); val != nil {
			ret = append(ret, val)
		}
	}
	return ret, nil
}

func (h *DefaultHandler) Lindex(key string, index int) ([]byte, error) {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}
	return h.brstack[key].GetIndex(index), nil
}

func (h *DefaultHandler) Lpush(key string, value []byte, values ...[]byte) (int, error) {
	values = append([][]byte{value}, values...)
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}
	for _, value := range values {
		h.brstack[key].PushFront(value)
	}
	return h.brstack[key].Len(), nil
}

func (h *DefaultHandler) Blpop(key string, keys ...string) (data [][]byte, err error) {
	keys = append([]string{key}, keys...)
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}

	if len(keys) == 0 {
		return nil, ErrParseTimeout
	}

	timeout, err := strconv.Atoi(keys[len(keys)-1])
	if err != nil {
		return nil, ErrParseTimeout
	}
	keys = keys[:len(keys)-1]

	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(time.Duration(timeout) * time.Second)
	} else {
		timeoutChan = make(chan time.Time)
	}

	finishedChan := make(chan struct{})

	go func() {
		defer close(finishedChan)
		selectCases := []reflect.SelectCase{}
		for _, k := range keys {
			key := string(k)
			if _, exists := h.brstack[key]; !exists {
				h.brstack[key] = NewStack(k)
			}
			selectCases = append(selectCases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(h.brstack[key].Chan),
			})
		}
		_, recv, _ := reflect.Select(selectCases)
		s, ok := recv.Interface().(*Stack)
		if !ok {
			err = fmt.Errorf("Impossible to retrieve data. Wrong type.")
			return
		}
		data = [][]byte{[]byte(s.Key), s.PopFront()}
	}()

	select {
	case <-finishedChan:
		return data, err
	case <-timeoutChan:
		return nil, nil
	}
	return nil, nil
}

func (h *DefaultHandler) Hget(key, subkey string) ([]byte, error) {
	if h.Database == nil || h.hvalues == nil {
		return nil, nil
	}

	if v, exists := h.hvalues[key]; exists {
		if v, exists := v[subkey]; exists {
			return v, nil
		}
	}
	return nil, nil
}

func (h *DefaultHandler) Hset(key, subkey string, value []byte) (int, error) {
	ret := 0

	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.hvalues[key]; !exists {
		h.hvalues[key] = make(HashValue)
		ret = 1
	}

	if _, exists := h.hvalues[key][subkey]; !exists {
		ret = 1
	}

	h.hvalues[key][subkey] = value

	return ret, nil
}

func (h *DefaultHandler) Hgetall(key string) (HashValue, error) {
	if h.Database == nil || h.hvalues == nil {
		return nil, nil
	}
	return h.hvalues[key], nil
}

func (h *DefaultHandler) Get(key string) ([]byte, error) {
	if h.Database == nil || h.values == nil {
		return nil, nil
	}
	return h.values[key], nil
}

func (h *DefaultHandler) Set(key string, value []byte) error {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	h.values[key] = value
	return nil
}

func (h *DefaultHandler) Del(key string, keys ...string) (int, error) {
	keys = append([]string{key}, keys...)
	if h.Database == nil {
		return 0, nil
	}
	count := 0
	for _, k := range keys {
		if _, exists := h.values[k]; exists {
			delete(h.values, k)
			count++
		}
		if _, exists := h.hvalues[key]; exists {
			delete(h.hvalues, k)
			count++
		}
	}
	return count, nil
}

func (h *DefaultHandler) Ping() (*StatusReply, error) {
	return &StatusReply{code: "PONG"}, nil
}

func (h *DefaultHandler) Subscribe(channels ...[]byte) (*MultiChannelWriter, error) {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
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

func (h *DefaultHandler) Publish(key string, value []byte) (int, error) {
	if h.Database == nil || h.sub == nil {
		return 0, nil
	}
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

func (h *DefaultHandler) Select(key string) error {
	if h.dbs == nil {
		h.dbs = map[int]*Database{0: h.Database}
	}
	index, err := strconv.Atoi(key)
	if err != nil {
		return err
	}
	h.dbs[h.currentDb] = h.Database
	h.currentDb = index
	if _, exists := h.dbs[index]; !exists {
		println("DB not exits, create ", index)
		h.dbs[index] = NewDatabase(nil)
	}
	h.Database = h.dbs[index]
	return nil
}

func (h *DefaultHandler) Monitor() (*MonitorReply, error) {
	return &MonitorReply{}, nil
}

func NewDefaultHandler() *DefaultHandler {
	db := NewDatabase(nil)
	ret := &DefaultHandler{
		Database:  db,
		currentDb: 0,
		dbs:       map[int]*Database{0: db},
	}
	return ret
}

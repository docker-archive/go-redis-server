package redis

import (
	"fmt"
	"reflect"
	"strconv"
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
	sub     HashSub
	brstack HashBrStack
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

func (h *DefaultHandler) RPUSH(key string, values ...[]byte) (int, error) {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack([]byte(key))
	}
	for _, value := range values {
		h.brstack[key].PushBash(value)
	}
	return len(h.brstack[key].stack), nil
}

func (h *DefaultHandler) BRPOP(keys ...[]byte) ([][]byte, error) {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}

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
		return nil, fmt.Errorf("Impossible to retrieve data. Wrong type.")
	}
	return [][]byte{s.Key, s.PopBack()}, nil
}

func (h *DefaultHandler) LPUSH(key string, values ...[]byte) (int, error) {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack([]byte(key))
	}
	for _, value := range values {
		h.brstack[key].PushFront(value)
	}
	return len(h.brstack[key].stack), nil
}

func (h *DefaultHandler) BLPOP(keys ...[]byte) ([][]byte, error) {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}

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
		return nil, fmt.Errorf("Impossible to retrieve data. Wrong type.")
	}
	return [][]byte{s.Key, s.PopFront()}, nil
}

func (h *DefaultHandler) HGET(key, subkey string) ([]byte, error) {
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

func (h *DefaultHandler) HSET(key, subkey string, value []byte) (int, error) {
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

func (h *DefaultHandler) HGETALL(key string) (HashValue, error) {
	if h.Database == nil || h.hvalues == nil {
		return nil, nil
	}
	return h.hvalues[key], nil
}

func (h *DefaultHandler) GET(key string) ([]byte, error) {
	if h.Database == nil || h.values == nil {
		return nil, nil
	}
	return h.values[key], nil
}

func (h *DefaultHandler) SET(key string, value []byte) error {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	h.values[key] = value
	return nil
}

func (h *DefaultHandler) DEL(keys ...[]byte) (int, error) {
	if h.Database == nil {
		return 0, nil
	}
	count := 0
	for _, k := range keys {
		key := string(k)
		if _, exists := h.values[key]; exists {
			delete(h.values, key)
			count++
		}
		if _, exists := h.hvalues[key]; exists {
			delete(h.hvalues, key)
			count++
		}
	}
	return count, nil
}

func (h *DefaultHandler) PING() (*StatusReply, error) {
	return &StatusReply{code: "PONG"}, nil
}

func (h *DefaultHandler) SUBSCRIBE(channels ...[]byte) (*MultiChannelWriter, error) {
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

func (h *DefaultHandler) PUBLISH(key string, value []byte) (int, error) {
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

func (h *DefaultHandler) SELECT(key string) error {
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

func (h *DefaultHandler) MONITOR() (*MonitorReply, error) {
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

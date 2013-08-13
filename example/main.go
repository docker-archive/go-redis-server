package main

import (
	redis "github.com/creack/go-redis-server"
)

type MyHandler struct {
	values map[string][]byte
	sub    map[string]*redis.ChannelWriter
}

func (h *MyHandler) GET(key string) ([]byte, error) {
	v, exists := h.values[key]
	if !exists {
		redis.Debugf("The requested key [%s] does not exist", key)
		return nil, nil
	}
	redis.Debugf("Getting key [%s] (%s)", key, v)
	return v, nil
}

func (h *MyHandler) SET(key string, value []byte) error {
	redis.Debugf("Setting key [%s] (%s)", key, value)
	h.values[key] = value
	return nil
}

func (h *MyHandler) SUBSCRIBE(key string, channels ...[]byte) (*redis.ChannelWriter, error) {
	redis.Debugf("SUBSCRIBE on %s\n", key)
	cw := &redis.ChannelWriter{
		FirstReply: []interface{}{
			"subscribe",
			key,
			1,
		},
		Channel: make(chan []interface{}),
	}
	h.sub[key] = cw
	return cw, nil
}

func (h *MyHandler) PUBLISH(key string, value []byte) (int, error) {
	//	redis.Debugf("Publishing %s on %s\n", value, key)
	v, exists := h.sub[key]
	if !exists {
		return 0, nil
	}
	v.Channel <- []interface{}{
		"message",
		key,
		value,
	}
	return 1, nil
}

func NewHandler() *MyHandler {
	return &MyHandler{
		values: make(map[string][]byte),
		sub:    make(map[string]*redis.ChannelWriter),
	}
}

func main() {
	handler, _ := redis.NewAutoHandler(NewHandler())
	server := &redis.Server{Proto: "tcp", Handler: handler, Addr: ":6389"}
	server.ListenAndServe()
}

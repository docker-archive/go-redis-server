package main

import (
	redis "github.com/dotcloud/go-redis-server"
)

type MyHandler struct {
	values map[string][]byte
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

func main() {
	handler, _ := redis.NewAutoHandler(&MyHandler{values: make(map[string][]byte)})
	server := &redis.Server{Handler: handler, Addr: ":6389"}
	server.ListenAndServe()
}

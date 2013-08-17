package main

import (
	"fmt"
	redis "github.com/dotcloud/go-redis-server"
)

type MyHandler struct {
	redis.DefaultHandler
}

func (h *MyHandler) GET(key string) ([]byte, error) {
	ret, err := h.DefaultHandler.GET(key)
	if ret == nil {
		return nil, err
	}
	return []byte("BEAM/" + string(ret)), err
}

func main() {
	defer func() {
		if msg := recover(); msg != nil {
			fmt.Printf("Panic: %v\n", msg)
		}
	}()

	handler, err := redis.NewAutoHandler(&MyHandler{})
	if err != nil {
		panic(err)
	}
	server := &redis.Server{Proto: "unix", Handler: handler, Addr: "/tmp/redis.sock"}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}

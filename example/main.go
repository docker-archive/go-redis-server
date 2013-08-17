package main

import (
	"fmt"
	redis "github.com/dotcloud/go-redis-server"
)

type MyHandler struct {
	redis.DefaultHandler
}

// TEST implement a new command. Non-redis standard, but it is possible.
func (h *MyHandler) Test() ([]byte, error) {
	return []byte("Awesome custom redis command!"), nil
}

// GET override the DefaultHandler's method.
func (h *MyHandler) Get(key string) ([]byte, error) {
	// However, we still can call the DefaultHandler GET method and use it.
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
	server := &redis.Server{
		Proto:   "unix",
		Handler: handler,
		Addr:    "/tmp/redis.sock",
	}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}

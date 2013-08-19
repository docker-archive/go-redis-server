package main

import (
	"fmt"
	redis "github.com/dotcloud/go-redis-server"
)

type MyHandler struct {
	redis.DefaultHandler
}

// Test implement a new command. Non-redis standard, but it is possible.
func (h *MyHandler) Test() ([]byte, error) {
	return []byte("Awesome custom redis command!"), nil
}

// Get override the DefaultHandler's method.
func (h *MyHandler) Get(key string) ([]byte, error) {
	// However, we still can call the DefaultHandler GET method and use it.
	ret, err := h.DefaultHandler.GET(key)
	if ret == nil {
		return nil, err
	}
	return []byte("BEAM/" + string(ret)), err
}

// Test2 implement a new command. Non-redis standard, but it is possible.
// This function needs to be registered.
func Test2() ([]byte, error) {
	return []byte("Awesome custom redis command via function!"), nil
}

func main() {
	defer func() {
		if msg := recover(); msg != nil {
			fmt.Printf("Panic: %v\n", msg)
		}
	}()

	myhandler := &MyHandler{}
	handler, err := redis.NewAutoHandler(myhandler)
	if err != nil {
		panic(err)
	}
	if err := handler.RegisterFct("test2", Test2); err != nil {
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

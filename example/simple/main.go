package main

import (
	redis "github.com/platinasystems/go-redis-server"
)

func main() {
	server, err := redis.NewServer(redis.DefaultConfig())
	if err != nil {
		panic(err)
	}
	if err := server.Start(); err != nil {
		panic(err)
	}
}

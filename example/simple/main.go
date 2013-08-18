package main

import (
	redis "github.com/dotcloud/go-redis-server"
)

func main() {
	server := &redis.Server{}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}

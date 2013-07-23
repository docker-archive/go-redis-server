package redis

import (
	"bufio"
	"fmt"
	"io"
)

// Serve starts a new redis session, using `conn` as a transport.
// It reads commands using the redis protocol, passes them to `handler`,
// and returns the result.
func Serve(conn io.ReadWriteCloser, handler *Handler) (err error) {
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
		conn.Close()
	}()
	reader := bufio.NewReader(conn)
	for {
		request, err := parseRequest(reader)
		if err != nil {
			return err
		}
		reply, err := Apply(handler, request)
		if err != nil {
			return err
		}
		_, err = reply.WriteTo(conn)
		if err != nil {
			return err
		}
	}
	return nil
}

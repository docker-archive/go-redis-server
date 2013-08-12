package redis

import (
	"bufio"
	"fmt"
	"io"
	"net"
)

type Server struct {
	Proto   string
	Addr    string   // TCP address to listen on, ":6389" if empty
	Handler *Handler // handler to invoke
}

func (srv *Server) ListenAndServe() error {
	addr := srv.Addr
	if srv.Proto == "" {
		srv.Proto = "tcp"
	}
	if srv.Proto == "unix" && addr == "" {
		addr = "/tmp/redis.sock"
	} else if addr == "" {
		addr = ":6389"
	}
	l, e := net.Listen(srv.Proto, addr)
	if e != nil {
		return e
	}
	return srv.Serve(l)
}

// Serve accepts incoming connections on the Listener l, creating a
// new service goroutine for each.  The service goroutines read requests and
// then call srv.Handler to reply to them.
func (srv *Server) Serve(l net.Listener) error {
	defer l.Close()
	for {
		rw, err := l.Accept()
		if err != nil {
			return err
		}
		go Serve(rw, srv.Handler)
	}
}

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

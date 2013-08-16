package redis

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
)

type Server struct {
	Proto   string
	Addr    string   // TCP address to listen on, ":6389" if empty
	Handler *Handler // handler to invoke
}

func (srv *Server) ListenAndServe() error {
	if srv.Handler == nil {
		return fmt.Errorf("nil handler")
	}
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
	if srv.Handler == nil {
		return fmt.Errorf("nil handler")
	}
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
	if handler == nil {
		return fmt.Errorf("nil handler")
	}
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
		conn.Close()
	}()
	reader := bufio.NewReader(conn)
	c := make(chan struct{})
	for {
		request, err := parseRequest(reader)
		if err != nil {
			return err
		}
		reply, err := Apply(handler, request, c)
		if err != nil {
			return err
		}
		// Read on `conn` in order to detect client disconnect
		go func() {
			// Close chan in order to trigger eventual selects
			defer close(c)
			defer conn.Close()
			defer Debugf("Client disconnected")
			io.Copy(ioutil.Discard, conn)
		}()
		_, err = reply.WriteTo(conn)
		if err != nil {
			return err
		}
	}
	return nil
}

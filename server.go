// go-redis-server is a helper library for building server software capable of speaking the redis protocol.
// This could be an alternate implementation of redis, a custom proxy to redis,
// or even a completely different backend capable of "masquerading" its API as a redis database.

package redis

import (
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
		h, err := NewAutoHandler(NewDefaultHandler())
		if err != nil {
			return err
		}
		srv.Handler = h
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
	monitorChan := []chan string{}
	for {
		rw, err := l.Accept()
		if err != nil {
			return err
		}
		go Serve(rw, srv.Handler, &monitorChan)
	}
}

// Serve starts a new redis session, using `conn` as a transport.
// It reads commands using the redis protocol, passes them to `handler`,
// and returns the result.
func Serve(conn net.Conn, handler *Handler, monitorChan *[]chan string) (err error) {
	if handler == nil {
		return fmt.Errorf("nil handler")
	}
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
		conn.Close()
	}()

	c := make(chan struct{})

	// Read on `conn` in order to detect client disconnect
	go func() {
		// Close chan in order to trigger eventual selects
		defer close(c)
		defer Debugf("Client disconnected")
		// FIXME: move conn within the request.
		if false {
			io.Copy(ioutil.Discard, conn)
		}
	}()

	var clientAddr string

	switch co := conn.(type) {
	case *net.UnixConn:
		f, err := conn.(*net.UnixConn).File()
		if err != nil {
			return err
		}
		clientAddr = f.Name()
	default:
		clientAddr = co.RemoteAddr().String()
	}

	for {
		request, err := parseRequest(conn)
		if err != nil {
			return err
		}
		request.Host = clientAddr

		reply, err := Apply(handler, request, c, monitorChan)
		if err != nil {
			return err
		}
		if _, err = reply.WriteTo(conn); err != nil {
			return err
		}
	}
	return nil
}

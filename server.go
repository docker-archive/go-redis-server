// go-redis-server is a helper library for building server software capable of speaking the redis protocol.
// This could be an alternate implementation of redis, a custom proxy to redis,
// or even a completely different backend capable of "masquerading" its API as a redis database.

package redis

import (
	"bufio"
	"fmt"
	"time"
	// "io"
	// "io/ioutil"
	"net"
	"reflect"
	"sync"
)

type Server struct {
	sync.Mutex
	Proto string // default, "tcp"
	Addr  string // default,
	// if Proto == unix then "/tmp/redis.sock" else ":6389"
	MonitorChans []chan string
	methods      map[string]HandlerFn
	listener     net.Listener
}

func (srv *Server) listen() error {
	addr := srv.Addr
	if srv.Proto == "" {
		srv.Proto = "tcp"
	}
	if addr == "" {
		if srv.Proto == "unix" {
			addr = "/tmp/redis.sock"
		} else {
			addr = ":6389"
		}
	}
	for i := 0; ; i++ {
		l, e := net.Listen(srv.Proto, addr)
		if e == nil {
			srv.listener = l
			break
		} else if i < 30 {
			// retry for devices that are still in ipv6
			// duplicate address detection
			time.Sleep(100 * time.Millisecond)
		} else {
			return e
		}
	}

	// if port was 0 and proto is tcp, the listener would use a random port
	srv.Addr = srv.listener.Addr().String()
	return nil
}

func (srv *Server) Start() error {
	return srv.Serve(srv.listener)
}

// Close shuts down the network port/socket
func (srv *Server) Close() error {
	if srv.listener == nil {
		return nil
	}
	return srv.listener.Close()
}

// Serve accepts incoming connections on the Listener l, creating a
// new service goroutine for each.  The service goroutines read requests and
// then call srv.Handler to reply to them.
func (srv *Server) Serve(l net.Listener) error {
	defer l.Close()
	srv.MonitorChans = []chan string{}
	for {
		rw, err := l.Accept()
		if err != nil {
			return err
		}
		go srv.ServeClient(rw)
	}
}

// Serve starts a new redis session, using `conn` as a transport.
// It reads commands using the redis protocol, passes them to `handler`,
// and returns the result.
func (srv *Server) ServeClient(conn net.Conn) (err error) {
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
		conn.Close()
	}()

	clientChan := make(chan struct{})

	// Read on `conn` in order to detect client disconnect
	/*
		go func() {
			// Close chan in order to trigger eventual selects
			defer close(clientChan)
			defer Debugf("Client disconnected")
			// FIXME: move conn within the request.
			if false {
				io.Copy(ioutil.Discard, conn)
			}
		}()
	*/

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

	reader := bufio.NewReader(conn)
	for {
		request, err := parseRequest(reader)
		if err != nil {
			return err
		}
		request.Host = clientAddr
		request.ClientChan = clientChan
		reply, err := srv.Apply(request)
		if err != nil {
			return err
		}
		if _, err = reply.WriteTo(conn); err != nil {
			return err
		}
	}
	return nil
}

func NewServer(c *Config) (*Server, error) {
	srv := &Server{
		Proto:        c.proto,
		MonitorChans: []chan string{},
		methods:      make(map[string]HandlerFn),
	}

	if srv.Proto == "unix" {
		srv.Addr = c.host
	} else {
		srv.Addr = fmt.Sprintf("%s:%d", c.host, c.port)
	}

	if c.handler == nil {
		c.handler = NewDefaultHandler()
	}

	rh := reflect.TypeOf(c.handler)
	for i := 0; i < rh.NumMethod(); i++ {
		method := rh.Method(i)
		if method.Name[0] >= 'a' && method.Name[0] <= 'z' {
			continue
		}
		handlerFn, err := srv.createHandlerFn(c.handler, &method.Func)
		if err != nil {
			return nil, err
		}
		srv.Register(method.Name, handlerFn)
	}

	err := srv.listen()
	if err != nil {
		return nil, err
	}
	return srv, nil
}

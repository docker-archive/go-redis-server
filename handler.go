package redis

import (
	"reflect"
	"strings"
)

type HandlerFn func(r *Request) (ReplyWriter, error)

func (srv *Server) RegisterFct(key string, f interface{}) error {
	v := reflect.ValueOf(f)
	handlerFn, err := srv.createHandlerFn(f, &v)
	if err != nil {
		return err
	}
	srv.Register(key, handlerFn)
	return nil
}

func (srv *Server) Register(name string, fn HandlerFn) {
	if srv.methods == nil {
		srv.methods = make(map[string]HandlerFn)
	}
	if fn != nil {
		Debugf("REGISTER: %s", strings.ToLower(name))
		srv.methods[strings.ToLower(name)] = fn
	}
}

func (srv *Server) Apply(r *Request) (ReplyWriter, error) {
	if srv == nil || srv.methods == nil {
		Debugf("The method map is uninitialized")
		return ErrMethodNotSupported, nil
	}
	fn, exists := srv.methods[strings.ToLower(r.Name)]
	if !exists {
		return ErrMethodNotSupported, nil
	}
	return fn(r)
}

func (srv *Server) ApplyString(r *Request) (string, error) {
	reply, err := srv.Apply(r)
	if err != nil {
		return "", err
	}
	return ReplyToString(reply)
}

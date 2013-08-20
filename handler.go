package redis

import (
	"reflect"
	"strings"
)

type HandlerFn func(r *Request, c chan struct{}, monitorChan *[]chan string) (ReplyWriter, error)

type Handler struct {
	methods map[string]HandlerFn
}

func (h *Handler) RegisterFct(key string, f interface{}) error {
	v := reflect.ValueOf(f)
	handlerFn, err := createHandlerFn(f, &v)
	if err != nil {
		return err
	}
	h.Register(key, handlerFn)
	return nil
}

func Apply(h *Handler, r *Request, c chan struct{}, monitorChan *[]chan string) (ReplyWriter, error) {
	if h == nil || h.methods == nil {
		Debugf("The method map is uninitialized")
		return ErrMethodNotSupported, nil
	}
	fn, exists := h.methods[strings.ToLower(r.Name)]
	if !exists {
		return ErrMethodNotSupported, nil
	}
	return fn(r, c, monitorChan)
}

func ApplyString(h *Handler, r *Request, c chan struct{}, monitorChan *[]chan string) (string, error) {
	reply, err := Apply(h, r, c, monitorChan)
	if err != nil {
		return "", err
	}
	return ReplyToString(reply)
}

func (h *Handler) Register(name string, fn HandlerFn) {
	if h.methods == nil {
		h.methods = make(map[string]HandlerFn)
	}
	Debugf("REGISTER: %s", strings.ToLower(name))
	h.methods[strings.ToLower(name)] = fn
}

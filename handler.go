package redis

import (
	"strings"
)

type HandlerFn func(r *Request, c chan struct{}) (ReplyWriter, error)

type Handler struct {
	methods map[string]HandlerFn
}

func Apply(h *Handler, r *Request, c chan struct{}) (ReplyWriter, error) {
	if h == nil || h.methods == nil {
		Debugf("The method map is uninitialized")
		return ErrMethodNotSupported, nil
	}
	fn, exists := h.methods[strings.ToLower(r.name)]
	if !exists {
		return ErrMethodNotSupported, nil
	}
	return fn(r, c)
}

func ApplyString(h *Handler, r *Request, c chan struct{}) (string, error) {
	reply, err := Apply(h, r, c)
	if err != nil {
		return "", err
	}
	return ReplyToString(reply)
}

func (h *Handler) Register(name string, fn HandlerFn) {
	if h.methods == nil {
		h.methods = make(map[string]HandlerFn)
	}
	h.methods[strings.ToLower(name)] = fn
}

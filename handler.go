package redis

import (
	"strings"
)

type HandlerFn func(r *Request) (ReplyWriter, error)

type Handler struct {
	methods map[string]HandlerFn
}

func Apply(h *Handler, r *Request) (ReplyWriter, error) {
	fn, exists := h.methods[strings.ToLower(r.name)]
	if !exists {
		return methodNotSupported(), nil
	}
	return fn(r)
}

func ApplyString(h *Handler, r *Request) (string, error) {
	reply, err := Apply(h, r)
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

package redis

import (
	"strings"
	"testing"
)

func TestEmptyHandler(t *testing.T) {
	c := make(chan struct{})
	defer close(c)
	reply, err := ApplyString(&Handler{}, &Request{}, c, &[]chan string{})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if !strings.Contains(reply, "-ERROR") {
		t.Fatalf("Eexpected error reply, got: %s", err)
	}
}

func TestCustomHandler(t *testing.T) {
	h := &Handler{}
	h.Register("GET", func(r *Request, c chan struct{}, monitorChan *[]chan string) (ReplyWriter, error) {
		return &BulkReply{value: []byte("42")}, nil
	})
	reply, err := ApplyString(h, &Request{Name: "gEt"}, nil, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	expected := "$2\r\n42\r\n"
	if reply != expected {
		t.Fatalf("Eexpected reply %q, got: %q", expected, reply)
	}
}

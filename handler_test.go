package redis

import (
	"strings"
	"testing"
)

func TestEmptyHandler(t *testing.T) {
	c := make(chan struct{})
	defer close(c)
	srv := &Server{}
	reply, err := srv.ApplyString(&Request{})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if !strings.Contains(reply, "-ERROR") {
		t.Fatalf("Eexpected error reply, got: %s", err)
	}
}

func TestCustomHandler(t *testing.T) {
	srv, err := NewServer(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	srv.Register("GET", func(r *Request) (ReplyWriter, error) {
		return &BulkReply{value: []byte("42")}, nil
	})
	reply, err := srv.ApplyString(&Request{Name: "gEt"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	expected := "$2\r\n42\r\n"
	if reply != expected {
		t.Fatalf("Eexpected reply %q, got: %q", expected, reply)
	}
}

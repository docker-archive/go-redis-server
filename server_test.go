package redis

import (
	"testing"
)

type TestHandler struct {
	DummyHandler
}

func (h *TestHandler) GET(key string) (*string, error) {
	res := "you asked for the key " + key
	return &res, nil
}

func TestApplyGET(t *testing.T) {
	t_input := "foo"
	t_output := "you asked for the key " + t_input
	result, err := ApplyString(new(TestHandler), "GET", t_input)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatalf("Expected '%s', got nil string", t_output)
	}
	if *result != t_output {
		t.Fatal("Expected '%s', got '%s'", t_output, *result)
	}
}

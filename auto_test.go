package redis

import (
	"testing"
)

type Hash struct {
	values map[string][]byte
}

type TestHandler struct {
	values     map[string][]byte
	hashValues map[string]Hash
}

func NewHandler() *TestHandler {
	return &TestHandler{
		values:     make(map[string][]byte),
		hashValues: make(map[string]Hash),
	}
}

func (h *TestHandler) GET(key string) ([]byte, error) {
	v, _ := h.values[key]
	return v, nil
}

func (h *TestHandler) SET(key string, value []byte) error {
	h.values[key] = value
	return nil
}

func (h *TestHandler) HMSET(key string, values *map[string][]byte) error {
	_, exists := h.hashValues[key]
	if !exists {
		h.hashValues[key] = Hash{values: make(map[string][]byte)}
	}
	hash := h.hashValues[key]
	for name, val := range *values {
		hash.values[name] = val
	}
	return nil
}

func (h *TestHandler) HGET(hash string, key string) ([]byte, error) {
	hs, exists := h.hashValues[hash]
	if !exists {
		return nil, nil
	}
	val, _ := hs.values[key]
	return val, nil
}

func TestAutoHandler(t *testing.T) {
	h, err := NewAutoHandler(NewHandler())
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	expected := []struct {
		request  *Request
		expected string
	}{
		{
			request: &Request{
				name: "GET",
				args: [][]byte{[]byte("key")},
			},
			expected: "$-1\r\n",
		},
		{
			request: &Request{
				name: "SET",
				args: [][]byte{
					[]byte("key"),
					[]byte("value"),
				},
			},
			expected: "+OK\r\n",
		},
		{
			request: &Request{
				name: "GET",
				args: [][]byte{[]byte("key")},
			},
			expected: "$5\r\nvalue\r\n",
		},
		{
			request: &Request{
				name: "HGET",
				args: [][]byte{
					[]byte("key"),
					[]byte("prop1"),
				},
			},
			expected: "$-1\r\n",
		},
		{
			request: &Request{
				name: "HMSET",
				args: [][]byte{
					[]byte("key"),
					[]byte("prop1"),
					[]byte("value1"),
					[]byte("prop2"),
					[]byte("value2"),
				},
			},
			expected: "+OK\r\n",
		},
		{
			request: &Request{
				name: "HGET",
				args: [][]byte{
					[]byte("key"),
					[]byte("prop1"),
				},
			},
			expected: "$6\r\nvalue1\r\n",
		},
		{
			request: &Request{
				name: "HGET",
				args: [][]byte{
					[]byte("key"),
					[]byte("prop2"),
				},
			},
			expected: "$6\r\nvalue2\r\n",
		},
	}
	for _, v := range expected {
		reply, err := ApplyString(h, v.request)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if reply != v.expected {
			t.Fatalf("Eexpected %q, got: %q", v.expected, reply)
		}
	}
}

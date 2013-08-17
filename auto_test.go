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

func (h *TestHandler) HMSET(key string, values map[string][]byte) error {
	_, exists := h.hashValues[key]
	if !exists {
		h.hashValues[key] = Hash{values: make(map[string][]byte)}
	}
	hash := h.hashValues[key]
	for name, val := range values {
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

func (h *TestHandler) HSET(hash string, key string, value []byte) error {
	_, exists := h.hashValues[hash]
	if !exists {
		h.hashValues[hash] = Hash{values: make(map[string][]byte)}
	}
	h.hashValues[hash].values[key] = value
	return nil
}

func (h *TestHandler) HGETALL(hash string) (map[string][]byte, error) {
	hs, exists := h.hashValues[hash]
	if !exists {
		return nil, nil
	}
	return hs.values, nil
}

func (h *TestHandler) BRPOP(key string, params ...[]byte) ([][]byte, error) {
	params = append(params, []byte(key))
	return params, nil
}

func (h *TestHandler) SUBSCRIBE(channel string, channels ...[]byte) (*ChannelWriter, error) {
	output := make(chan []interface{})
	writer := &ChannelWriter{
		FirstReply: []interface{}{
			[]byte("subscribe"), // []byte
			channel,             // string
			1,                   // int
		},
		Channel: output,
	}
	go func() {
		output <- []interface{}{
			[]byte("message"),
			channel,
			[]byte("yo"),
		}
		close(output)
	}()
	return writer, nil
}

func (h *TestHandler) DEL(key string, keys ...[]byte) (int, error) {
	var deleted int
	deleteKey := func(k string) {
		_, exists := h.values[k]
		if exists {
			deleted += 1
			delete(h.values, k)
		}
		_, exists = h.hashValues[k]
		if exists {
			deleted += 1
			delete(h.hashValues, k)
		}
	}

	deleteKey(key)
	for _, k := range keys {
		deleteKey(string(k))
	}

	return deleted, nil
}

func TestAutoHandler(t *testing.T) {
	h, err := NewAutoHandler(NewHandler())
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	expected := []struct {
		request  *Request
		expected []string
	}{
		{
			request: &Request{
				name: "GET",
				args: [][]byte{[]byte("key")},
			},
			expected: []string{"$-1\r\n"},
		},
		{
			request: &Request{
				name: "SET",
				args: [][]byte{
					[]byte("key"),
					[]byte("value"),
				},
			},
			expected: []string{"+OK\r\n"},
		},
		{
			request: &Request{
				name: "GET",
				args: [][]byte{[]byte("key")},
			},
			expected: []string{"$5\r\nvalue\r\n"},
		},
		{
			request: &Request{
				name: "HGET",
				args: [][]byte{
					[]byte("hkey"),
					[]byte("prop1"),
				},
			},
			expected: []string{"$-1\r\n"},
		},
		{
			request: &Request{
				name: "HMSET",
				args: [][]byte{
					[]byte("hkey"),
					[]byte("prop1"),
					[]byte("value1"),
					[]byte("prop2"),
					[]byte("value2"),
				},
			},
			expected: []string{"+OK\r\n"},
		},
		{
			request: &Request{
				name: "HGET",
				args: [][]byte{
					[]byte("hkey"),
					[]byte("prop1"),
				},
			},
			expected: []string{"$6\r\nvalue1\r\n"},
		},
		{
			request: &Request{
				name: "HGET",
				args: [][]byte{
					[]byte("hkey"),
					[]byte("prop2"),
				},
			},
			expected: []string{"$6\r\nvalue2\r\n"},
		},
		{
			request: &Request{
				name: "HGETALL",
				args: [][]byte{
					[]byte("hkey"),
				},
			},
			expected: []string{
				"*4\r\n$5\r\nprop1\r\n$6\r\nvalue1\r\n$5\r\nprop2\r\n$6\r\nvalue2\r\n",
				"*4\r\n$5\r\nprop2\r\n$6\r\nvalue2\r\n$5\r\nprop1\r\n$6\r\nvalue1\r\n",
			},
		},
		{
			request: &Request{
				name: "HSET",
				args: [][]byte{
					[]byte("hkey"),
					[]byte("prop1"),
					[]byte("newvalue"),
				},
			},
			expected: []string{
				"+OK\r\n",
			},
		},
		{
			request: &Request{
				name: "HGET",
				args: [][]byte{
					[]byte("hkey"),
					[]byte("prop1"),
				},
			},
			expected: []string{"$8\r\nnewvalue\r\n"},
		},
		{
			request: &Request{
				name: "DEL",
				args: [][]byte{
					[]byte("key"),
					[]byte("hkey"),
				},
			},
			expected: []string{":2\r\n"},
		},
		{
			request: &Request{
				name: "BRPOP",
				args: [][]byte{
					[]byte("bkey"),
				},
			},
			expected: []string{
				"*1\r\n$4\r\nbkey\r\n",
			},
		},
		{
			request: &Request{
				name: "BRPOP",
				args: [][]byte{
					[]byte("key1"),
					[]byte("key2"),
				},
			},
			expected: []string{
				"*2\r\n$4\r\nkey2\r\n$4\r\nkey1\r\n",
			},
		},
		{
			request: &Request{
				name: "SUBSCRIBE",
				args: [][]byte{
					[]byte("foo"),
				},
			},
			expected: []string{
				"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$2\r\nyo\r\n",
			},
		},
	}
	for _, v := range expected {
		c := make(chan struct{})
		reply, err := ApplyString(h, v.request, c)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		match := false
		for _, expected := range v.expected {
			if reply == expected {
				match = true
				break
			}
		}
		if match == false {
			t.Fatalf("Eexpected one of %q, got: %q", v.expected, reply)
		}
		close(c)
	}
}

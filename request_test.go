package redis

import (
	"bytes"
	"testing"
)

func TestRequestExpectArgument(t *testing.T) {
	r := &Request{Name: "Hi", Args: [][]byte{}}
	for i := 0; i < 10; i += 1 {
		reply := r.ExpectArgument(i)
		if reply == nil {
			t.Fatalf("Expected error reply, got nil")
		}
	}
	r = &Request{Name: "Hi", Args: [][]byte{{'h', 'i'}}}
	reply := r.ExpectArgument(0)
	if reply != nil {
		t.Fatalf("Expected nil reply, got %s", reply)
	}

	reply = r.ExpectArgument(1)
	if reply == nil {
		t.Fatalf("Expected error reply, got nil")
	}
}

func TestRequestGetString(t *testing.T) {
	s := "Hello, World!"
	r := &Request{Name: "Hi", Args: [][]byte{[]byte(s)}}
	val, reply := r.GetString(0)
	if reply != nil {
		t.Fatalf("Expected nil reply, got %s", reply)
	}
	if val != s {
		t.Fatalf("Expected %s, got %s", s, val)
	}
	val, reply = r.GetString(5)
	if reply == nil {
		t.Fatalf("Expected reply, got nil")
	}
}

func TestRequestGetInteger(t *testing.T) {
	invalid := []*Request{
		{Name: "Hi", Args: [][]byte{}},
		{Name: "Hi", Args: [][]byte{{'h', 'i'}}},
	}
	for _, request := range invalid {
		_, reply := request.GetInteger(0)
		if reply == nil {
			t.Fatalf("Expected error reply, got nil")
		}
	}

	valid := []struct {
		request *Request
		index   int
		number  int
	}{
		{&Request{Name: "Hi", Args: [][]byte{{'1'}}}, 0, 1},
		{&Request{Name: "Hi", Args: [][]byte{{'1'}, []byte("42")}}, 1, 42},
		{&Request{Name: "Hi", Args: [][]byte{{'1'}, []byte("-1043")}}, 1, -1043},
	}
	for _, v := range valid {
		number, reply := v.request.GetInteger(v.index)
		if reply != nil {
			t.Fatalf("Expected nil reply, got %s", reply)
		}
		if v.number != number {
			t.Fatalf("Expected %d reply, got %d", number, v.number)
		}
		number, reply = v.request.GetPositiveInteger(v.index)
		if v.number > 0 {
			if reply != nil {
				t.Fatalf("Expected nil reply, got %s", reply)
			}
		} else {
			if reply == nil {
				t.Fatalf("Expected error reply, got %s", reply)
			}
		}
	}
}

func TestRequestGetMap(t *testing.T) {
	invalid := []struct {
		request *Request
		index   int
	}{
		{&Request{Name: "Hi", Args: [][]byte{}}, 0},
		{&Request{Name: "Hi", Args: [][]byte{}}, 100},
		{&Request{Name: "Hi", Args: [][]byte{{'h', 'i'}}}, 0},
		{&Request{Name: "Hi", Args: [][]byte{{'h', 'i'}, {'h', 'i'}, {'h', 'i'}}}, 0},
	}
	for _, v := range invalid {
		_, reply := v.request.GetMap(v.index)
		if reply == nil {
			t.Fatalf("Expected error reply, got nil for %s %d", v.request, v.index)
		}
	}

	valid := []struct {
		request  *Request
		index    int
		expected map[string][]byte
	}{
		{
			request: &Request{
				Name: "Hi",
				Args: [][]byte{
					{'h', 'i'},
					{'y', 'o'},
				},
			},
			index:    0,
			expected: map[string][]byte{"hi": []byte("yo")},
		},
		{
			request: &Request{
				Name: "Hi",
				Args: [][]byte{
					[]byte("hi"),
					[]byte("yo"),
					[]byte("key"),
					[]byte("value"),
				},
			},
			index:    0,
			expected: map[string][]byte{"hi": []byte("yo"), "key": []byte("value")},
		},
	}
	for _, v := range valid {
		m, reply := v.request.GetMap(v.index)
		if reply != nil {
			t.Fatalf("Expected nil reply, got %s for %s %d", reply, v.request, v.index)
		}
		if !mapsEqual(m, v.expected) {
			t.Fatalf("Expected %s got %s for %s", v.expected, v.request, m)
		}
	}
}

func mapsEqual(a map[string][]byte, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for key, val := range a {
		if !bytes.Equal(b[key], val) {
			return false
		}
	}
	return true
}

package redis

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"testing"
)

func TestReader(t *testing.T) {
	v, err := ioutil.ReadAll(r("Hello!"))
	if err != nil {
		t.Fatalf("Should have read it actually")
	}
	if string(v) != "Hello!" {
		t.Fatalf("Expected %s here, got %s", "Hello!", v)
	}
}

func TestParseBadRequests(t *testing.T) {
	requests := []string{
		//empty lines,
		"", "\r\n", "\r\r", "\n\n",
		// malformed start line,
		"hello\r\n", " \r\n", "*hello\r\n", "*-100\r\n",
		//malformed arguments count
		"*3\r\nhi", "*3\r\nhi\r\n", "*4\r\n$1", "*4\r\n$1\r", "*4\r\n$1\n",
		//Corrupted argument size
		"*2\r\n$3\r\ngEt\r\n$what?\r\nx\r\n",
		//mismatched arguments count
		"*4\r\n$3\r\ngEt\r\n$1\r\nx\r\n",
		//missing trailing \r\n
		"*2\r\n$3\r\ngEt\r\n$1\r\nx",
		//missing trailing \r\n
		"*2\r\n$3\r\ngEt\r\n$1\r\nx\r",
		//lied about argument length \r\n
		"*2\r\n$3\r\ngEt\r\n$100\r\nx\r\n",
	}
	for _, v := range requests {
		_, err := parseRequest(r(v))
		if err == nil {
			t.Fatalf("Expected eror %s", v)
		}
	}
}

func TestSucess(t *testing.T) {
	expected := []struct {
		r Request
		s string
	}{
		{Request{name: "a"}, "*1\r\n$1\r\na\r\n"},
		{Request{name: "get"}, "*1\r\n$3\r\ngEt\r\n"},
		{Request{name: "get", args: b("x")}, "*2\r\n$3\r\ngEt\r\n$1\r\nx\r\n"},
		{Request{name: "set", args: b("mykey", "myvalue")}, "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"},
	}

	for _, p := range expected {
		request, err := parseRequest(r(p.s))
		if err != nil {
			t.Fatalf("Un xxpected eror %s when parsting", err, p.s)
		}
		if request.name != p.r.name {
			t.Fatalf("Expected command %s, got %s", err, p.r.name, request.name)
		}
		if len(request.args) != len(p.r.args) {
			t.Fatalf("Args length mismatch %s, got %s", err, p.r.args, request.args)
		}
		for i := 0; i < len(request.args); i += 1 {
			if !bytes.Equal(request.args[i], p.r.args[i]) {
				t.Fatalf("Expected args %s, got %s", err, p.r.args, request.args)
			}
		}
	}
}

func b(args ...string) [][]byte {
	arr := make([][]byte, len(args))
	for i := 0; i < len(args); i += 1 {
		arr[i] = []byte(args[i])
	}
	return arr
}

func r(request string) *bufio.Reader {
	return bufio.NewReader(bytes.NewReader([]byte(request)))
}

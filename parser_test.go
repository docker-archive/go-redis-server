package redis

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"strings"
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
		// malformed start line,
		"*hello\r\n", "*-100\r\n",
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
		_, err := parseRequest(ioutil.NopCloser(strings.NewReader(v)))
		if err == nil {
			t.Fatalf("Expected error for request [%s]", v)
		}
	}
}

func TestSucess(t *testing.T) {
	expected := []struct {
		r Request
		s string
	}{
		{Request{Name: "a"}, "*1\r\n$1\r\na\r\n"},
		{Request{Name: "get"}, "*1\r\n$3\r\ngEt\r\n"},
		{Request{Name: "get", Args: b("x")}, "*2\r\n$3\r\ngEt\r\n$1\r\nx\r\n"},
		{Request{Name: "set", Args: b("mykey", "myvalue")}, "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"},
	}

	for _, p := range expected {
		request, err := parseRequest(ioutil.NopCloser(strings.NewReader(p.s)))
		if err != nil {
			t.Fatalf("Un xxpected eror %s when parsting", err, p.s)
		}
		if request.Name != p.r.Name {
			t.Fatalf("Expected command %s, got %s", err, p.r.Name, request.Name)
		}
		if len(request.Args) != len(p.r.Args) {
			t.Fatalf("Args length mismatch %s, got %s", err, p.r.Args, request.Args)
		}
		for i := 0; i < len(request.Args); i += 1 {
			if !bytes.Equal(request.Args[i], p.r.Args[i]) {
				t.Fatalf("Expected args %s, got %s", err, p.r.Args, request.Args)
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

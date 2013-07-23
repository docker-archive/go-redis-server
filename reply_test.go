package redis

import (
	"bytes"
	"testing"
)

func TestWriteStatus(t *testing.T) {
	replies := []struct {
		reply    ReplyWriter
		expected string
	}{
		{&StatusReply{code: "OK"}, "+OK\r\n"},
		{&IntegerReply{number: 42}, ":42\r\n"},
		{&ErrorReply{code: "ERROR", message: "Something went wrong"}, "-ERROR Something went wrong\r\n"},
		{&BulkReply{}, "$-1\r\n"},
		{&BulkReply{[]byte{'h', 'e', 'l', 'l', 'o'}}, "$5\r\nhello\r\n"},
		{&MultiBulkReply{[][]byte{[]byte{'h', 'e', 'l', 'l', 'o'}}}, "*1\r\n$5\r\nhello\r\n"},
		{&MultiBulkReply{[][]byte{[]byte{'h', 'e', 'l', 'l', 'o'}, []byte{'h', 'i'}}}, "*2\r\n$5\r\nhello\r\n$2\r\nhi\r\n"},
		{&MultiBulkReply{[][]byte{nil, []byte{'h', 'e', 'l', 'l', 'o'}, nil, []byte{'h', 'i'}}}, "*4\r\n$-1\r\n$5\r\nhello\r\n$-1\r\n$2\r\nhi\r\n"},
	}
	for _, p := range replies {
		var b bytes.Buffer
		n, err := p.reply.WriteTo(&b)
		if err != nil {
			t.Fatalf("Oops, unexpected %s", err)
		}
		val := b.String()
		if val != p.expected {
			t.Fatalf("Oops, expected %q, got %q instead", p.expected, val)
		}
		if n != len(p.expected) {
			t.Fatalf("Expected to write %d bytes, wrote %d instead", len(p.expected), n)
		}
	}
}

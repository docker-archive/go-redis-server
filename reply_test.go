package redis

import (
	"bytes"
	"errors"
	"io"
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
		{&MultiBulkReply{[]interface{}{[]byte{'h', 'e', 'l', 'l', 'o'}}}, "*1\r\n$5\r\nhello\r\n"},
		{&MultiBulkReply{[]interface{}{[]byte{'h', 'e', 'l', 'l', 'o'}, []byte{'h', 'i'}}}, "*2\r\n$5\r\nhello\r\n$2\r\nhi\r\n"},
		{&MultiBulkReply{[]interface{}{nil, []byte{'h', 'e', 'l', 'l', 'o'}, nil, []byte{'h', 'i'}}}, "*4\r\n$-1\r\n$5\r\nhello\r\n$-1\r\n$2\r\nhi\r\n"},
		{MultiBulkFromMap(map[string]interface{}{"hello": []byte("there"), "how": []byte("are you")}), "*4\r\n$5\r\nhello\r\n$5\r\nthere\r\n$3\r\nhow\r\n$7\r\nare you\r\n"},
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
		if n != int64(len(p.expected)) {
			t.Fatalf("Expected to write %d bytes, wrote %d instead", len(p.expected), n)
		}
	}
}

func TestWriteBytes(t *testing.T) {
	// Note: we test only failure here. Success is already tested.
	if _, err := writeBytes([]byte("Hello World!"), NewFailWriter(1)); err == nil {
		t.Fatal("Error expected after 1 write")
	}
	if _, err := writeBytes([]byte("Hello World!"), NewFailWriter(2)); err == nil {
		t.Fatal("Error expected after 2 writes")
	}
}

func TestWriteMultiBytes(t *testing.T) {
	// Note: we test only failure here. Success is already tested.
	if _, err := writeMultiBytes(nil, nil); err == nil {
		t.Fatal("Expect error when writing `nil`")
	}
	if _, err := writeMultiBytes([]interface{}{[]byte("Hello World!")}, NewFailWriter(1)); err == nil {
		t.Fatal("Error expected after 1 write")
	}
	if _, err := writeMultiBytes([]interface{}{[]byte("Hello World!")}, NewFailWriter(2)); err == nil {
		t.Fatal("Error expected after 2 write")
	}
}

type FailWriter struct {
	io.ReadWriter
	n int
}

func (fw *FailWriter) Write(buf []byte) (int, error) {
	fw.n -= 1
	if fw.n > 0 {
		return fw.ReadWriter.Write(buf)
	}
	return 0, errors.New("FAILED")
}

// NewFailWriter instanciate a new writer that will fail after n write.
func NewFailWriter(n int) io.ReadWriter {
	w := bytes.NewBuffer([]byte{})
	return &FailWriter{
		ReadWriter: w,
		n:          n,
	}
}

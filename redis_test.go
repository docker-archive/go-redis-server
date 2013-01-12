package livedb

import (
	"testing"
)

var testData map[string]interface{} = map[string]interface{}{
	"foo":	"bar",
	"h":	map[string]string {
		"ga": "bu",
	},
}



func TestNewFrom(t *testing.T) {
	if !NewFrom(testData).FieldEquals("foo", "bar") {
		t.Fatalf("NewFrom")
	}
}

func TestSET(t *testing.T) {
	db := New()
	db.SET("foo", "bar")
	if !db.FieldEquals("foo", "bar") {
		t.Fatalf("%s != %s", db.data["foo"], "bar")
	}
}

func TestApplySET(t *testing.T) {
	db := New()
	_, err := db.Apply("SET", "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}
	if !db.FieldEquals("foo", "bar") {
		t.Fatal("GET/SET")
	}
}

func testGETExists(t *testing.T) {
	db := NewFrom(map[string]interface{}{"foo":"bar"})
	if value, err := db.GET("foo"); err != nil {
		t.Fatal(err)
	} else if !stringEqual(value, "bar") {
		t.Fatalf("GET returned %s instead of bar", value)
	}
}

func TestGETNonExist(t *testing.T) {
	db := New()
	if value, err := db.GET("foo"); err != nil {
		t.Fatal("GET on non-existing key should return nil, not an error")
	} else if value != nil {
		t.Fatal("GET on non-existing key should return nil")
	}
}

func TestGETonHash(t *testing.T) {
	db := New()
	db.HSET("foo", "k", "hello")
	if _, err := db.GET("foo"); err == nil {
		t.Fatal("GET on hash should return an error")
	}
}

func TestHSET(t *testing.T) {
	db := New()
	db.HSET("foo", "k", "hello")
	if !db.Equals(NewFromJSON([]byte("{\"foo\": {\"k\": \"hello\"}}"))) {
		t.Fatalf("Wrong db state after HSET: %#v\n", db.data)
	}
}

func TestHGET(t *testing.T) {
	db := NewFrom(testData)
	if value, err := db.HGET("h", "ga"); err != nil {
		t.Fatal(err)
	} else if !stringEqual(value, "bu") {
		t.Fatalf("HGET returned %s intead of %s", value, "bu")
	}
}

func TestHGETNonExistingHash(t *testing.T) {
	db := New()
	if value, err := db.HGET("foo", "k"); err != nil {
		t.Fatal("HGET on non-existing hash should return null")
	} else if value != nil {
		t.Fatal("HGET on non-existing hash should return null")
	}
}

func TestHGETNonExistingKey(t *testing.T) {
	db := New()
	if value, err := db.HGET("foo", "k"); err != nil {
		t.Fatal(err)
	} else if value != nil {
		t.Fatal("HGET on non-existing key should return null")
	}
}

func TestLoadJSON1(t *testing.T) {
	db := New()
	if n, err := db.LoadJSON([]byte("{\"foo\": \"bar\"}")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("LoadJSON returned %d instead of 1", n)
	}
	if value, err := db.GET("foo"); err != nil {
		t.Fatal(err)
	} else if value == nil {
		t.Fatalf("LoadJSON didn't store a value")
	} else if *value != "bar" {
		t.Fatalf("LoadJSON stored the wrong value (%s)", *value)
	}
}

func TestLoadJSON2(t *testing.T) {
	db := New()
	if n, err := db.LoadJSON([]byte("{\"foo\": {\"ga\": \"bu\"}}")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("LoadJSON returned %d instead of 1", n)
	}
	if !db.FieldEquals("foo", map[string]string{"ga": "bu"}) {
		t.Fatalf("Wrong DB state after LoadJSON: %#v\n", db.data)
	}
}

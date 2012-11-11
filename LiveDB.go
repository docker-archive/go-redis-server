
package livedb

import (
    "strings"
    "encoding/json"
    "log"
)

/*
** DB
*/


type DBFeed chan DBOp

type DBOp struct {
    Op      string  // SET or DEL
    Key     string
    Value   interface{}
}


func (op *DBOp) Apply(db *DB) {
    switch strings.ToUpper(op.Op) {
        case "SET": {
            db.Set(op.Key, op.Value)
        }
        case "DEL": {
            db.Delete(op.Key)
        }
    }
}


func ParseOp(s string) (*DBOp, error) {
    var op DBOp
    err := json.Unmarshal([]byte(s), &op)
    if err != nil {
        return nil, err
    }
    return &op, nil
}


func (op *DBOp) String() (string, error) {
    s, err := json.Marshal(op)
    if err != nil {
        return "", err
    }
    return string(s), nil
}



type DB struct {
    data        map[string]interface{}
    subscribers []chan DBOp
}


func New() (*DB) {
    return &DB{
        make(map[string]interface{}),
        []chan DBOp{},
    }
}


func (db *DB) Json() string {
    s, err := json.Marshal(db.data)
    if err != nil {
        log.Fatal(err)
    }
    return string(s)
}

func (m *DB) Get(key string) interface{} {
    return m.data[key]
}

func (m *DB) Set(key string, value interface{}) {
    for _, sub := range m.subscribers {
        sub <- DBOp{"SET", key, value,}
    }
    m.data[key] = value
}

func (db *DB) Delete(key string) {
    for _, sub := range db.subscribers {
        sub <- DBOp{"DEL", key, nil,}
    }
    delete(db.data, key)
}


func (m *DB) Subscribe() DBFeed {
    feed := make(chan DBOp)
    m.subscribers = append(m.subscribers, feed)
    return feed
}

func (db *DB) UnsubscribeAll() {
    for _, sub := range db.subscribers {
        close(sub)
    }
    db.subscribers = []chan DBOp{}
}


func (m *DB) WatchKey(key string) chan *interface{} {
    feed := make(chan *interface{})
    lock := make(chan bool)
    go func() {
        defer close(feed)
        defer close(lock)
        for op := range m.Subscribe() {
            if op.Key != key {
                continue
            }
            if op.Op == "DEL" {
                return
            } else if op.Op == "SET" {
                feed <- &op.Value
            }
        }
    }()
    <-lock
    return feed
}

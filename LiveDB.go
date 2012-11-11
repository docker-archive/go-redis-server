
package livedb

import (
    "strings"
    "encoding/json"
    "log"
)

/*
** LiveDB
*/


type LiveDBFeed chan LiveDBOp

type LiveDBOp struct {
    Op      string  // SET or DEL
    Key     string
    Value   interface{}
}


func (op *LiveDBOp) Apply(db *LiveDB) {
    switch strings.ToUpper(op.Op) {
        case "SET": {
            db.Set(op.Key, op.Value)
        }
        case "DEL": {
            db.Delete(op.Key)
        }
    }
}


func ParseOp(s string) (*LiveDBOp, error) {
    var op LiveDBOp
    err := json.Unmarshal([]byte(s), &op)
    if err != nil {
        return nil, err
    }
    return &op, nil
}


func (op *LiveDBOp) String() (string, error) {
    s, err := json.Marshal(op)
    if err != nil {
        return "", err
    }
    return string(s), nil
}



type LiveDB struct {
    data        map[string]interface{}
    subscribers []chan LiveDBOp
}


func NewLiveDB() (*LiveDB) {
    return &LiveDB{
        make(map[string]interface{}),
        []chan LiveDBOp{},
    }
}


func (db *LiveDB) Json() string {
    s, err := json.Marshal(db.data)
    if err != nil {
        log.Fatal(err)
    }
    return string(s)
}

func (m *LiveDB) Get(key string) interface{} {
    return m.data[key]
}

func (m *LiveDB) Set(key string, value interface{}) {
    for _, sub := range m.subscribers {
        sub <- LiveDBOp{"SET", key, value,}
    }
    m.data[key] = value
}

func (db *LiveDB) Delete(key string) {
    for _, sub := range db.subscribers {
        sub <- LiveDBOp{"DEL", key, nil,}
    }
    delete(db.data, key)
}


func (m *LiveDB) Subscribe() LiveDBFeed {
    feed := make(chan LiveDBOp)
    m.subscribers = append(m.subscribers, feed)
    return feed
}

func (db *LiveDB) UnsubscribeAll() {
    for _, sub := range db.subscribers {
        close(sub)
    }
    db.subscribers = []chan LiveDBOp{}
}


func (m *LiveDB) WatchKey(key string) chan *interface{} {
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

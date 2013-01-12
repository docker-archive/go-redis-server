
package livedb

import (
    "bufio"
    "io"
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
        case "LOAD": {
            db.Load(op.Value.(map[string]interface{}))
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
    replicating bool
}



func NewDB() (*DB) {
    return &DB{
        data:   make(map[string]interface{}),
    }
}


func (db *DB) Send() {
    data := db.Data()
    // FIXME: serialize replication operations to avoid race condition
    db.replicating = true
    db.dispatch(DBOp{"LOAD", "", data,})
}


func (db *DB) Data() map[string]interface{} {
    return db.data
}


func (db *DB) Json() string {
    s, err := json.Marshal(db.data)
    if err != nil {
        log.Fatal(err)
    }
    return string(s)
}


func (db *DB) Load(data map[string] interface{}) {
    for key := range db.data {
        delete(db.data, key)
    }
    for key, value := range data {
        db.data[key] = value
    }
}

func (db *DB) LoadJson(s string) error {
    data := make(map[string]interface{})
    err := json.Unmarshal([]byte(s), &data)
    if err != nil {
        return err
    }
    db.Load(data)
    return nil
}

func (m *DB) Get(key string) interface{} {
    return m.data[key]
}

func (m *DB) Set(key string, value interface{}) {
    m.dispatch(DBOp{"SET", key, value,})
    m.data[key] = value
}

func (db *DB) Delete(key string) {
    db.dispatch(DBOp{"DEL", key, nil,})
    delete(db.data, key)
}


func (db *DB) dispatch(op DBOp) {
    if !db.replicating {
        return
    }
    for _, sub := range db.subscribers {
        sub <- op
    }
}


func (m *DB) Subscribe() DBFeed {
    feed := make(chan DBOp, 1)
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

func (feed *DBFeed) Apply(db *DB) {
    for {
        op, ok := <-*feed
        if !ok {
            return
        }
        op.Apply(db)
    }
}


/*
 *
 */

func (db *DB) ReplicateTo(dest io.Writer) error {
    for op := range db.Subscribe() {
        var err error
        opString, err := op.String()
        if err != nil {
            return err
        }
        opBytes := []byte(opString + "\n")
        _, err = dest.Write(opBytes)
        if err != nil {
            return err
        }
    }
    return nil
}

func (db *DB) ReplicateFrom(src io.Reader) error {
    lines := bufio.NewReader(src)
    for {
        opBytes, _, err := lines.ReadLine()
        if err  != nil && err != io.EOF {
            return err
        }
        eof := (err == io.EOF)
        op, err := ParseOp(string(opBytes))
        if err != nil {
            return err
        }
        op.Apply(db)
        if eof {
            return nil
        }
    }
    db.UnsubscribeAll()
   return nil
}

package livedb

import (
	"errors"
	"reflect"
	"strings"
	"fmt"
	"encoding/json"
	"io"
)

type RedisDB struct {
	data map[string]interface{}
}

func New() *RedisDB {
	return &RedisDB{data: make(map[string]interface{})}
}

func NewFrom(data map[string]interface{}) *RedisDB {
	db := New()
	db.LoadData(data)
	return db
}

func NewFromJSON(jsonData []byte) *RedisDB {
	db := New()
	db.LoadJSON(jsonData)
	return db
}

func (db *RedisDB) Equals(other *RedisDB) bool {
	return db.Includes(other) && other.Includes(db)
}

func (db *RedisDB) Includes(other *RedisDB) bool {
	for key := range other.data {
		if value, exists := db.data[key]; !exists {
			return false
		} else {
			if !other.FieldEquals(key, value) {
				return false
			}
		}
	}
	return true
}

func (db *RedisDB) FieldEquals(key string, otherValue interface{}) bool {
	if value, exists := db.data[key]; exists {
		if Sothervalue, ok := otherValue.(*string); ok {
			if Svalue, ok := value.(*string); ok {
				return stringEqual(Svalue, Sothervalue)
			}
			return false
		}
		if Sothervalue, ok := otherValue.(string); ok {
			if Svalue, ok := value.(*string); ok {
				return stringEqual(&Sothervalue, Svalue)
			}
			return false
		}
		if Hothervalue, ok := otherValue.(map[string]string); ok {
			if Hvalue, ok := value.(map[string]string); ok {
				return hashEqual(Hvalue, Hothervalue)
			}
			return false
		}
	}
	return false
}

func checkMethodSignature(method *reflect.Method, nArgs int) error {
	errorType := reflect.TypeOf(checkMethodSignature).Out(0)
	mtype := method.Func.Type()
	// Check input
	if mtype.IsVariadic() && mtype.In(mtype.NumIn() - 1) != reflect.TypeOf([]string{}) {
		return errors.New("Variadic argument is not []string")
	}
	if nArgs < mtype.NumIn() - 1 {
		return errors.New("Not enough arguments")
	}
	if nArgs > mtype.NumIn() - 1 {
		return errors.New("Too many arguments")
	}
	for i:=1; i<mtype.NumIn(); i+=1 {
		if mtype.In(i) != reflect.TypeOf("") {
			return errors.New(fmt.Sprintf("Argument %d: wrong type %s", i, mtype.In(i)))
		}
	}
	// Check output
	if mtype.NumOut() == 0 {
		return errors.New("Not enough return values")
	}
	if mtype.NumOut() > 2 {
		return errors.New("Too many return values")
	}
	if t := mtype.Out(mtype.NumOut() - 1); t != errorType {
		return errors.New(fmt.Sprintf("Last return value must be an error (not %s)", t))
	}
	return nil
}

func (db *RedisDB) ReplicateFrom(src io.Reader) (int, error) {
	var nCommands int
	if n, err := db.LoadGob(src); err != nil {
		return n, err
	}
	reader := NewReader(src)
	for {
		if cmd, key, args, err := reader.Read(); err == io.EOF {
			return nCommands, nil
		} else if err != nil {
			return nCommands, err
		} else {
			if _, err := db.Apply(cmd, key, args...); err != nil {
				return nCommands, err
			}
		}
		nCommands += 1
	}
	return nCommands, nil
}

func (db *RedisDB) ReplicateTo(dst io.Writer) (int, error) {
	// FIXME: accumulate commands
	dump := NewDump(db.data)
	if err := dump.Encode(dst); err != nil {
		return 0, err
	}
	// FIXME: send all future commands
	return 0, nil
}

func (db *RedisDB) Apply(cmd, key string, args ... string) (interface{}, error) {
	method, exists := reflect.TypeOf(db).MethodByName(strings.ToUpper(cmd))
	if !exists {
		return nil, errors.New(fmt.Sprintf("%s: no such command", cmd))
	}
	if err := checkMethodSignature(&method, len(args) + 1); err != nil {
		return nil, err
	}
	input := []reflect.Value{reflect.ValueOf(db), reflect.ValueOf(key)}
	var result []reflect.Value
	mType := method.Func.Type()
	if mType.IsVariadic() {
		for i:=0; i<mType.NumIn(); i+=1 {
			input = append(input, reflect.ValueOf(args[i]))
		}
		input = append(input, reflect.ValueOf(args[mType.NumIn():]))
		result = method.Func.CallSlice(input)
	} else {
		for _, arg := range args {
			input = append(input, reflect.ValueOf(arg))
		}
		result = method.Func.Call(input)
	}
	var ret interface{}
	var err error
	// Last return value is an error
	if ierr := result[len(result) - 1].Interface(); ierr != nil {
		err = ierr.(error)
	}
	if len(result) == 1 {
		ret = result[0].Interface()
	}
	return ret, err
}

func convertString(value interface{}) (string, error) {
	switch v := value.(type) {
		case string:	return v, nil
		case *string:	{ if v != nil { return *v, nil }}
		case int:	return fmt.Sprintf("%d", v), nil
		case bool:	{ if v { return "1", nil } else { return "0", nil }}
	}
	return "", errors.New("Unsupported type")
}

func convertHash(value interface{}) (map[string]string, error) {
	switch v := value.(type) {
		case map[string]string:	return v, nil
		case map[string]interface{}: {
			hash := make(map[string]string)
			for key, ifield := range v {
				if field, err := convertString(ifield); err != nil {
					return nil, err
				} else {
					hash[key] = field
				}
			}
			return hash, nil
		}
	}
	return nil, errors.New("Unsupported type")
}

func (db *RedisDB) LoadData(data map[string]interface{}) (int, error) {
	for key := range db.data { // FIXME: use a pointer to reset the whole map instead
		delete(db.data, key)
	}
	var stored int
	for key, value := range data {
		if sValue, err := convertString(value); err == nil {
			db.data[key] = &sValue
		} else if hValue, err := convertHash(value); err == nil {
			db.data[key] = hValue
		} else {
			continue
		}
		stored += 1
	}
	return stored, nil
}

func (db *RedisDB) LoadGob(src io.Reader) (int, error) {
	dump, err := DecodeDump(src)
	if err != nil {
		return 0, err
	}
	return db.LoadData(dump.Data())

}

func (db *RedisDB) DumpGob(dst io.Writer) error {
	return NewDump(db.data).Encode(dst)
}

func (db *RedisDB) LoadJSON(jsonData []byte) (int, error) {
	data := make(map[string]interface{})
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return 0, err
	}
	return db.LoadData(data)
}


func (db *RedisDB) JSON() ([]byte, error) {
	return json.Marshal(db.data)
}

func (db *RedisDB) getBytes(key string) (*string, error)  {
	value, exists := db.data[key]
	if !exists {
		return nil, nil
	}
	if bytes, ok := value.(*string); ok {
		return bytes, nil
	}
	return nil, errors.New(fmt.Sprintf("Value is not a binary string (%s)", reflect.TypeOf(value)))
}

func (db *RedisDB) EXISTS(key string) bool {
	_, exists := db.data[key]
	return exists
}


func (db *RedisDB) SET(key, value string) error {
	db.data[key] = &value
	return nil
}

func (db *RedisDB) GET(key string) (*string, error) {
	return db.getBytes(key)
}

func (db *RedisDB) DEL(keys ...string) int {
	var nDeleted int
	for _, key := range keys {
		if db.EXISTS(key) {
			delete(db.data, key)
			nDeleted += 1
		}
	}
	return nDeleted
}

func (db *RedisDB) APPEND(key string, value string) (int, error) {
	if oldValue, err := db.GET(key); err != nil {
		return 0, err
	} else if oldValue == nil {
		db.SET(key, value)
	} else {
		db.SET(key, *oldValue + value)
	}
	return len(*(db.data[key].(*string))), nil
}

func (db *RedisDB) getHash(key string) (map[string]string, error) {
	value, exists := db.data[key]
	if !exists {
		return nil, nil
	}
	if hash, ok := value.(map[string]string); ok {
		return hash, nil
	}
	return nil, errors.New("Not a hash")
}

func (db *RedisDB) HSET(key, field, value string) (int, error) {
	hash, err := db.getHash(key)
	if err != nil {
		return 0, err
	}
	if hash == nil {
		hash = make(map[string]string)
		db.data[key] = hash
	}
	var result int
	if _, exists := hash[field]; exists {
		result = 0
	} else {
		result = 1
	}
	hash[field] = value
	return result, nil
}

func (db *RedisDB) HGET(key, field string) (*string, error) {
	hash, err := db.getHash(key)
	if err != nil {
		return nil, err
	}
	if hash == nil {
		return nil, nil
	}
	if value, exists := hash[field]; exists {
		return &value, nil
	}
	return nil, nil
}


package redis

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"time"
)

type CheckerFn func(request *Request) (reflect.Value, ReplyWriter)

// type AutoHandler interface {
// 	GET(key string) ([]byte, error)
// 	SET(key string, value []byte) error
// 	HMSET(key string, values *map[string][]byte) error
// 	HGETALL(key string) (*map[string][]byte, error)
// 	HGET(hash string, key string) ([]byte, error)
// 	HSET(hash string, key string, value []byte) error
// 	BRPOP(channels ...[]byte) ([][]byte, error)
// 	SUBSCRIBE(channels ...[]byte) (*ChannelWriter, error)
// 	DEL(key string, keys ...[]byte) (int, error)
// }

func NewAutoHandler(autoHandler interface{}) (*Handler, error) {
	handler := &Handler{}

	rh := reflect.TypeOf(autoHandler)
	for i := 0; i < rh.NumMethod(); i++ {
		method := rh.Method(i)
		handlerFn, err := createHandlerFn(autoHandler, &method)
		if err != nil {
			return nil, err
		}
		handler.Register(method.Name, handlerFn)
	}
	return handler, nil
}

func createHandlerFn(autoHandler interface{}, method *reflect.Method) (HandlerFn, error) {
	errorType := reflect.TypeOf(createHandlerFn).Out(1)
	mtype := method.Func.Type()
	checkers, err := createCheckers(method)
	if err != nil {
		return nil, err
	}

	// Check output
	if mtype.NumOut() == 0 {
		return nil, errors.New("Not enough return values")
	}
	if mtype.NumOut() > 2 {
		return nil, errors.New("Too many return values")
	}
	if t := mtype.Out(mtype.NumOut() - 1); t != errorType {
		return nil, fmt.Errorf("Last return value must be an error (not %s)", t)
	}

	return handlerFn(autoHandler, method, checkers)
}

func handlerFn(autoHandler interface{}, method *reflect.Method, checkers []CheckerFn) (HandlerFn, error) {
	return func(request *Request, c chan struct{}, monitorChans *[]chan string) (ReplyWriter, error) {
		input := []reflect.Value{reflect.ValueOf(autoHandler)}

		for _, checker := range checkers {
			value, reply := checker(request)
			if reply != nil {
				return reply, nil
			}
			input = append(input, value)
		}
		monitorString := fmt.Sprintf("%.6f [0 %s] \"%s\" \"%s\"",
			float64(time.Now().UTC().UnixNano())/1e9,
			request.Host,
			request.Name,
			bytes.Join(request.Args, []byte{'"', ' ', '"'}))
		for _, c := range *monitorChans {
			select {
			case c <- monitorString:
			default:
			}
		}
		Debugf("Monitors: %d\n", len(*monitorChans))
		Debugf("%s\n", monitorString)
		var result []reflect.Value
		if method.Func.Type().IsVariadic() {
			result = method.Func.CallSlice(input)
		} else {
			result = method.Func.Call(input)
		}

		var ret interface{}
		if ierr := result[len(result)-1].Interface(); ierr != nil {
			// Last return value is an error, wrap it to redis error
			err := ierr.(error)
			// convert to redis error reply
			return NewError(err.Error()), nil
		}
		if len(result) > 1 {
			ret = result[0].Interface()
			return createReply(ret, c, monitorChans)
		}
		return &StatusReply{code: "OK"}, nil
	}, nil
}

func hashValueReply(v HashValue) (*MultiBulkReply, error) {
	m := make(map[string]interface{})
	for k, v := range v {
		m[k] = v
	}
	return MultiBulkFromMap(m), nil
}

func createReply(val interface{}, c chan struct{}, monitorChans *[]chan string) (ReplyWriter, error) {
	Debugf("CREATE REPLY: %T", val)
	switch v := val.(type) {
	case []interface{}:
		return &MultiBulkReply{values: v}, nil
	case string:
		return &BulkReply{value: []byte(v)}, nil
	case [][]byte:
		if v, ok := val.([]interface{}); ok {
			return &MultiBulkReply{values: v}, nil
		}
		m := make([]interface{}, len(v), cap(v))
		for i, elem := range v {
			m[i] = elem
		}
		return &MultiBulkReply{values: m}, nil
	case []byte:
		return &BulkReply{value: v}, nil
	case HashValue:
		return hashValueReply(v)
	case map[string][]byte:
		return hashValueReply(v)
	case map[string]interface{}:
		return MultiBulkFromMap(v), nil
	case int:
		return &IntegerReply{number: v}, nil
	case *StatusReply:
		return v, nil
	case *MonitorReply:
		c := make(chan string)
		*monitorChans = append(*monitorChans, c)
		println("len monitor: ", len(*monitorChans))
		v.c = c
		return v, nil
	case *ChannelWriter:
		return v, nil
	case *MultiChannelWriter:
		println("New client")
		for _, mcw := range v.Chans {
			mcw.clientChan = c
		}
		return v, nil
	default:
		return nil, fmt.Errorf("Unsupported type: %s (%T)", v, v)
	}
}

func createCheckers(method *reflect.Method) ([]CheckerFn, error) {
	checkers := []CheckerFn{}
	mtype := method.Func.Type()
	for i := 1; i < mtype.NumIn(); i += 1 {
		switch mtype.In(i) {
		case reflect.TypeOf(""):
			checkers = append(checkers, stringChecker(i-1))
		case reflect.TypeOf([]byte{}):
			checkers = append(checkers, byteChecker(i-1))
		case reflect.TypeOf([][]byte{}):
			checkers = append(checkers, byteSliceChecker(i-1))
		case reflect.TypeOf(map[string][]byte{}):
			if i != mtype.NumIn()-1 {
				return nil, errors.New("Map should be the last argument")
			}
			checkers = append(checkers, mapChecker(i-1))
		case reflect.TypeOf(1):
			checkers = append(checkers, intChecker(i-1))
		default:
			return nil, fmt.Errorf("Argument %d: wrong type %s (%s)", i, mtype.In(i), method.Name)
		}
	}
	return checkers, nil
}

func stringChecker(index int) CheckerFn {
	return func(request *Request) (reflect.Value, ReplyWriter) {
		v, err := request.GetString(index)
		if err != nil {
			return reflect.ValueOf(""), err
		}
		return reflect.ValueOf(v), nil
	}
}

func byteChecker(index int) CheckerFn {
	return func(request *Request) (reflect.Value, ReplyWriter) {
		err := request.ExpectArgument(index)
		if err != nil {
			return reflect.ValueOf([]byte{}), err
		}
		return reflect.ValueOf(request.Args[index]), nil
	}
}

func byteSliceChecker(index int) CheckerFn {
	return func(request *Request) (reflect.Value, ReplyWriter) {
		if !request.HasArgument(index) {
			return reflect.ValueOf([][]byte{}), nil
		} else {
			return reflect.ValueOf(request.Args[index:]), nil
		}
	}
}

func mapChecker(index int) CheckerFn {
	return func(request *Request) (reflect.Value, ReplyWriter) {
		m, err := request.GetMap(index)
		return reflect.ValueOf(m), err
	}
}

func intChecker(index int) CheckerFn {
	return func(request *Request) (reflect.Value, ReplyWriter) {
		m, err := request.GetInteger(index)
		return reflect.ValueOf(m), err
	}
}

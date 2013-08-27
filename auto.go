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
// 	BRPOP(channel string, channels ...string) ([][]byte, error)
// 	SUBSCRIBE(channel string, channels ...string) (*ChannelWriter, error)
// 	DEL(key string, keys ...string) (int, error)
// }

func (srv *Server) createHandlerFn(autoHandler interface{}, f *reflect.Value) (HandlerFn, error) {
	errorType := reflect.TypeOf(srv.createHandlerFn).Out(1)
	mtype := f.Type()
	checkers, err := createCheckers(autoHandler, f)
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

	return srv.handlerFn(autoHandler, f, checkers)
}

func (srv *Server) handlerFn(autoHandler interface{}, f *reflect.Value, checkers []CheckerFn) (HandlerFn, error) {
	return func(request *Request) (ReplyWriter, error) {
		input := []reflect.Value{reflect.ValueOf(autoHandler)}

		for _, checker := range checkers {
			value, reply := checker(request)
			if reply != nil {
				return reply, nil
			}
			input = append(input, value)
		}
		var monitorString string
		if len(request.Args) > 0 {
			monitorString = fmt.Sprintf("%.6f [0 %s] \"%s\" \"%s\"",
				float64(time.Now().UTC().UnixNano())/1e9,
				request.Host,
				request.Name,
				bytes.Join(request.Args, []byte{'"', ' ', '"'}))
		} else {
			monitorString = fmt.Sprintf("%.6f [0 %s] \"%s\"",
				float64(time.Now().UTC().UnixNano())/1e9,
				request.Host,
				request.Name)
		}
		for _, c := range srv.MonitorChans {
			select {
			case c <- monitorString:
			default:
			}
		}
		Debugf("%s (connected monitors: %d)\n", monitorString, len(srv.MonitorChans))

		var result []reflect.Value

		// If we don't have any input, it means we are dealing with a function.
		// Then remove the first parameter (object instance)
		if f.Type().NumIn() == 0 {
			input = []reflect.Value{}
		} else if f.Type().In(0).AssignableTo(reflect.TypeOf(autoHandler)) == false {
			// If we have at least one input, we check if the first one is an instance of our object
			// If it is, then remove it from the input list.
			input = input[1:]
		}

		if f.Type().IsVariadic() {
			result = f.CallSlice(input)
		} else {
			result = f.Call(input)
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
			return srv.createReply(request, ret)
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

func (srv *Server) createReply(r *Request, val interface{}) (ReplyWriter, error) {
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
		srv.MonitorChans = append(srv.MonitorChans, c)
		println("len monitor: ", len(srv.MonitorChans))
		v.c = c
		return v, nil
	case *ChannelWriter:
		return v, nil
	case *MultiChannelWriter:
		println("New client")
		for _, mcw := range v.Chans {
			mcw.clientChan = r.ClientChan
		}
		return v, nil
	default:
		return nil, fmt.Errorf("Unsupported type: %s (%T)", v, v)
	}
}

func createCheckers(autoHandler interface{}, f *reflect.Value) ([]CheckerFn, error) {
	checkers := []CheckerFn{}
	mtype := f.Type()

	start := 0
	// If we are dealing with a method, start at 1 (first being the instance)
	// Otherwise, start at 0.
	if mtype.NumIn() > 0 && mtype.In(0).AssignableTo(reflect.TypeOf(autoHandler)) {
		start = 1
	}

	for i := start; i < mtype.NumIn(); i += 1 {
		switch mtype.In(i) {
		case reflect.TypeOf(""):
			checkers = append(checkers, stringChecker(i-start))
		case reflect.TypeOf([]string{}):
			checkers = append(checkers, stringSliceChecker(i-start))
		case reflect.TypeOf([]byte{}):
			checkers = append(checkers, byteChecker(i-start))
		case reflect.TypeOf([][]byte{}):
			checkers = append(checkers, byteSliceChecker(i-start))
		case reflect.TypeOf(map[string][]byte{}):
			if i != mtype.NumIn()-1 {
				return nil, errors.New("Map should be the last argument")
			}
			checkers = append(checkers, mapChecker(i-start))
		case reflect.TypeOf(1):
			checkers = append(checkers, intChecker(i-start))
		default:
			return nil, fmt.Errorf("Argument %d: wrong type %s (%s)", i, mtype.In(i), mtype.Name())
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

func stringSliceChecker(index int) CheckerFn {
	return func(request *Request) (reflect.Value, ReplyWriter) {
		if !request.HasArgument(index) {
			return reflect.ValueOf([]string{}), nil
		} else {
			v, err := request.GetStringSlice(index)
			if err != nil {
				return reflect.ValueOf([]string{}), err
			}
			return reflect.ValueOf(v), nil
		}
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

package redis

import (
	"errors"
	"fmt"
	"reflect"
)

type AutoHandler interface {
	GET(key string) ([]byte, error)
	SET(key string, value []byte) error
	HMSET(key string, values *map[string][]byte) error
	HGETALL(key string) (*map[string][]byte, error)
	HGET(hash string, key string) ([]byte, error)
	HSET(hash string, key string, value []byte) error
	BRPOP(key string, params ...[]byte) ([][]byte, error)
	SUBSCRIBE(channel string, channels ...[]byte) (*ChannelWriter, error)
	DEL(key string, keys ...[]byte) (int, error)
}

func NewAutoHandler(autoHandler AutoHandler) (*Handler, error) {
	handler := &Handler{}

	rh := reflect.TypeOf(autoHandler)
	for i := 0; i < rh.NumMethod(); i += 1 {
		method := rh.Method(i)
		handlerFn, err := createHandlerFn(autoHandler, &method)
		if err != nil {
			return nil, err
		}
		handler.Register(method.Name, handlerFn)
	}
	return handler, nil
}

func createHandlerFn(autoHandler AutoHandler, method *reflect.Method) (HandlerFn, error) {
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
		return nil, errors.New(
			fmt.Sprintf("Last return value must be an error (not %s)", t))
	}

	return handlerFn(autoHandler, method, checkers)
}

func handlerFn(autoHandler AutoHandler, method *reflect.Method, checkers []CheckerFn) (HandlerFn, error) {
	return func(request *Request) (ReplyWriter, error) {
		input := []reflect.Value{reflect.ValueOf(autoHandler)}
		for _, checker := range checkers {
			value, reply := checker(request)
			if reply != nil {
				return reply, nil
			}
			input = append(input, value)
		}

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
			return createReply(ret)
		}
		return &StatusReply{code: "OK"}, nil
	}, nil
}

func createReply(val interface{}) (ReplyWriter, error) {
	switch val := val.(type) {
	case [][]byte:
		return &MultiBulkReply{values: val}, nil
	case string:
		return &BulkReply{value: []byte(val)}, nil
	case []byte:
		return &BulkReply{value: val}, nil
	case *map[string][]byte:
		return MultiBulkFromMap(val), nil
	case int:
		return &IntegerReply{number: val}, nil
	case *ChannelWriter:
		return val, nil
	default:
		return nil, errors.New(fmt.Sprintf("Unsupported type: %s", val))
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
		case reflect.TypeOf(&map[string][]byte{}):
			if i != mtype.NumIn()-1 {
				return nil, errors.New("Map should be the last argument")
			}
			checkers = append(checkers, mapChecker(i-1))
		case reflect.TypeOf(1):
			checkers = append(checkers, intChecker(i-1))
		default:
			return nil, errors.New(
				fmt.Sprintf("Argument %d: wrong type %s", i, mtype.In(i)))
		}
	}
	return checkers, nil
}

type CheckerFn func(request *Request) (reflect.Value, ReplyWriter)

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
		return reflect.ValueOf(request.args[index]), nil
	}
}

func byteSliceChecker(index int) CheckerFn {
	return func(request *Request) (reflect.Value, ReplyWriter) {
		if !request.HasArgument(index) {
			return reflect.ValueOf([][]byte{}), nil
		} else {
			return reflect.ValueOf(request.args[index:]), nil
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

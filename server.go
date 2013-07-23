package redis

import (
	"os"
	"errors"
	"fmt"
	"reflect"
	"bufio"
	"io"
	"io/ioutil"
	"runtime"
	"net"
	"strings"
)

type Handler interface {
	GET(key string) (*string, error)
	SET(key, value string) error
}

type DummyHandler struct {
}

func (h *DummyHandler) GET(key string) (*string, error) {
	result := "42"
	return &result, nil
}

func (h *DummyHandler) SET(key, value string) error {
	return nil
}

func Serve(conn net.Conn, handler Handler) (err error) {
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
		conn.Close()
	}()
	reader := bufio.NewReader(conn)
	for {
		// FIXME: commit the current container before each command
		Debugf("Reading command...")
		var nArg int
		line, err := reader.ReadString('\r')
		if err != nil {
			return err
		}
		Debugf("line == '%s'", line)
		if len(line) < 1 || line[len(line) - 1] != '\r' {
			return fmt.Errorf("Malformed request: doesn't start with '*<nArg>\\r\\n'. %s", err)
		}
		line = line[:len(line) - 1]
		if _, err := fmt.Sscanf(line, "*%d", &nArg); err != nil {
			return fmt.Errorf("Malformed request: '%s' doesn't start with '*<nArg>'. %s", line, err)
		}
		Debugf("nArg = %d", nArg)
		nl := make([]byte, 1)
		if _, err := reader.Read(nl); err != nil {
			return err
		} else if nl[0] != '\n' {
			return fmt.Errorf("Malformed request: expected '%x', got '%x'", '\n', nl[0])
		}
		var (
			opName string
			opArgs []string
		)
		for i:=0; i<nArg; i+=1 {
			Debugf("\n-------\nReading arg %d/%d", i + 1, nArg)
			// FIXME: specify int size?
			var argSize int64

			line, err := reader.ReadString('\r')
			if err != nil {
				return err
			}
			Debugf("line == '%s'", line)
			if len(line) < 1 || line[len(line) - 1] != '\r' {
				return fmt.Errorf("Malformed request: doesn't start with '$<nArg>\\r\\n'. %s", err)
			}
			line = line[:len(line) - 1]
			if _, err := fmt.Sscanf(line, "$%d", &argSize); err != nil {
				return fmt.Errorf("Malformed request: '%s' doesn't start with '$<nArg>'. %s", line, err)
			}
			Debugf("argSize= %d", argSize)
			nl := make([]byte, 1)
			if _, err := reader.Read(nl); err != nil {
				return err
			} else if nl[0] != '\n' {
				return fmt.Errorf("Malformed request: expected '%x', got '%x'", '\n', nl[0])
			}


			// Read arg data
			argData, err := ioutil.ReadAll(io.LimitReader(reader, argSize + 2))
			if err != nil {
				return err
			} else if n := int64(len(argData)); n < argSize + 2 {
				return fmt.Errorf("Malformed request: argument data #%d doesn't match declared size (expected %d bytes (%d + \r\n), read %d)", i, argSize + 2, argSize, n)
			} else if string(argData[len(argData) - 2:]) != "\r\n" {
				return fmt.Errorf("Malformed request: argument #%d doesn't end with \\r\\n", i)
			}
			arg := string(argData[:len(argData) - 2])
			Debugf("arg = %s", arg)
			if i == 0 {
				opName = strings.ToLower(arg)
			} else {
				opArgs = append(opArgs, arg)
			}
		}
		result, err := Apply(handler, opName, opArgs...)
		if err != nil {
			return err
		}
		fmt.Fprintf(conn, "+%s\n", result)
	}
	return nil
}

func ApplyString(handler Handler, cmd string, args ... string) (*string, error) {
	IResult, err := Apply(handler, cmd, args...)
	if err != nil {
		return nil, err
	}
	if IResult == nil {
		return nil, err
	}
	result, isString := IResult.(*string)
	if !isString {
		return nil, fmt.Errorf("Result is not a string")
	}
	return result, nil
}

// Apply parses and executes a redis command
func Apply(handler Handler, cmd string, args ... string) (interface{}, error) {
	method, exists := reflect.TypeOf(handler).MethodByName(strings.ToUpper(cmd))
	if !exists {
		return nil, errors.New(fmt.Sprintf("%s: no such command", cmd))
	}
	Debugf("Method = %v", method)
	if err := checkMethodSignature(&method, len(args)); err != nil {
		return nil, err
	}
	input := []reflect.Value{reflect.ValueOf(handler)}
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
	if len(result) > 1 {
		ret = result[0].Interface()
	}
	return ret, err
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

// Debug function, if the debug flag is set, then display. Do nothing otherwise
// If Docker is in damon mode, also send the debug info on the socket
func Debugf(format string, a ...interface{}) {
	if os.Getenv("DEBUG") != "" {

		// Retrieve the stack infos
		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "<unknown>"
			line = -1
		} else {
			file = file[strings.LastIndex(file, "/")+1:]
		}

		fmt.Fprintf(os.Stderr, fmt.Sprintf("[%d] [debug] %s:%d %s\n", os.Getpid(), file, line, format), a...)
	}
}


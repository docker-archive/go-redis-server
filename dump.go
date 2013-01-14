package livedb

import (
	"encoding/gob"
	"io"
)


type Dump struct {
	Strings	map[string] *string
	Hashes	map[string] map[string]string
}

func NewDump(data map[string]interface{}) *Dump {
	dump := Dump{
		Strings: make(map[string]*string),
		Hashes: make(map[string]map[string]string),
	}
	for key, val := range data {
		switch vv := val.(type) {
			case *string:		dump.Strings[key] = vv
			case map[string]string:	dump.Hashes[key] = vv
		}
	}
	return &dump
}

func (d *Dump) Data() map[string]interface{} {
	data := make(map[string]interface{})
	for key, val := range d.Strings {
		data[key] = val
	}
	for key, val := range d.Hashes {
		data[key] = val
	}
	return data
}

func (d *Dump) Encode(dst io.Writer) error {
	return gob.NewEncoder(dst).Encode(d)
}

func DecodeDump(src io.Reader) (*Dump, error) {
	var d Dump
	if err := gob.NewDecoder(src).Decode(&d); err != nil {
		return nil, err
	}
	return &d, nil
}

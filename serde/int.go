package serde

import (
	"fmt"
	"reflect"
	"strconv"
)

type IntSerde struct{}

func NewIntSerde() *IntSerde {
	return &IntSerde{}
}

func (i *IntSerde) Encode(v interface{}) ([]byte, error) {

	in, ok := v.(int)
	if !ok {
		return nil, fmt.Errorf("invalid type [%v]", reflect.TypeOf(v).String())
	}
	return []byte(strconv.Itoa(in)), nil
}

func (i *IntSerde) Decode(data []byte) (interface{}, error) {
	in, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, fmt.Errorf("cannot decode data err: %s", err.Error())
	}
	return in, nil
}

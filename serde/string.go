package serde

import (
	"fmt"
	"reflect"
)

type StringSerde struct{}

func NewStringSerde() *StringSerde {
	return &StringSerde{}
}

func (s *StringSerde) Serialize(v interface{}) ([]byte, error) {
	str, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf(`invalid type [%+v]`, reflect.TypeOf(v))
	}
	return []byte(str), nil
}

func (s *StringSerde) Decode(data []byte) (interface{}, error) {
	return string(data), nil
}

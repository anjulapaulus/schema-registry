package serde

import "encoding/json"

type JsonSerde struct{}

func NewJsonSerde() *JsonSerde {
	return &JsonSerde{}
}

func (s *JsonSerde) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (s *JsonSerde) Decode(byt []byte, v interface{}) error {
	return json.Unmarshal(byt, &v)

}

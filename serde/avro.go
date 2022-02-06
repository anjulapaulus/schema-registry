package serde

import "github.com/hamba/avro"

type AvroSerde struct {
	API avro.API
}

type Schema avro.Schema

func NewAvroSerde() *AvroSerde {
	return &AvroSerde{
		API: avro.DefaultConfig,
	}
}

func (a *AvroSerde) Parse(schema string) (Schema, error) {
	return avro.Parse(schema)
}

func (a *AvroSerde) Marshal(schema Schema, value interface{}) ([]byte, error) {
	return a.API.Marshal(schema, value)
}

func (a *AvroSerde) Unmarshal(schema Schema, data []byte, value interface{}) error {
	return a.API.Unmarshal(schema, data, value)
}

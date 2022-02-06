package schemaregistry

const (
	contentType = "application/vnd.schemaregistry.v1+json"
)

type SchemaType string

const (
	AVRO       SchemaType = "AVRO"
	PROTOBUF   SchemaType = "PROTOBUF"
	JSONSCHEMA SchemaType = "JSONSCHEMA"
)

type Version int

const LatestVersion Version = -1
const AllVersions Version = -2

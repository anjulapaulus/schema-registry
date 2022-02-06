package schemaregistry

type Schema struct {
	ID         int
	Schema     string
	SchemaType *SchemaType
	Subject    string
	Version    int
	References []Reference
}

type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

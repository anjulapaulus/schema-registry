package schemaregistry

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
)

type registryOptions struct {
	client HTTPClient
}

type regOps func(*registryOptions)

func WithHTTPClient(client HTTPClient) regOps {
	return func(opts *registryOptions) {
		opts.client = client
	}
}

type SchemaRegistry struct {
	subjectVersionSchema map[string]map[int]*Schema
	idSchema             map[int]*Schema
	ssMu                 *sync.RWMutex
	idMu                 *sync.RWMutex
	registry             *SchemaClient
}

func NewRegistry(url string, ops ...regOps) (*SchemaRegistry, error) {
	if url == "" {
		return nil, errors.New("schema-registry.NewRegistry Error: url empty")
	}
	opts := &registryOptions{}
	for _, opt := range ops {
		opt(opts)
	}
	var client *SchemaClient
	client, err := NewClient(url)
	if err != nil {
		return nil, err

	}
	if opts.client != nil {
		client, err = NewClient(url, WithCustomHTTPClient(opts.client))
		if err != nil {
			return nil, err
		}
	}

	r := SchemaRegistry{
		subjectVersionSchema: make(map[string]map[int]*Schema),
		idSchema:             make(map[int]*Schema),
		ssMu:                 new(sync.RWMutex),
		idMu:                 new(sync.RWMutex),
		registry:             client,
		// cache:                opts.cache,
	}

	return &r, nil
}

func (sr *SchemaRegistry) Register(subject string, version int) error {
	if subject == "" || version == 0 {
		return errors.New("subject and version can be empty")
	}
	sr.ssMu.RLock()
	if cSchema, ok := sr.subjectVersionSchema[subject][version]; ok {
		if cSchema == nil {
			return fmt.Errorf(`subject:%s version:%d registered but schema is nil`, subject, version)
		}
		return fmt.Errorf(`subject:%s version:%d already registered`, subject, version)
	}
	sr.ssMu.RUnlock()

	resp, err := sr.registry.GetSchemaByVersion(subject, version)
	if err != nil {
		return fmt.Errorf(`error registering schema err:%s`, err.Error())
	}

	schema := &Schema{
		ID:         resp.ID,
		Schema:     resp.Schema,
		SchemaType: resp.SchemaType,
		Subject:    resp.Subject,
		Version:    resp.Version,
	}

	sr.ssMu.Lock()
	sr.subjectVersionSchema[subject][version] = schema
	sr.ssMu.Unlock()

	sr.idMu.Lock()
	sr.idSchema[resp.ID] = schema
	sr.idMu.Unlock()

	return nil
}

func (sr *SchemaRegistry) GetSchemaByID(schemaId int) (*Schema, error) {
	if schemaId == 0 {
		return nil, errors.New("schema id cannot be zero")
	}

	sr.idMu.RLock()
	if cSchema, ok := sr.idSchema[schemaId]; ok {
		return cSchema, nil
	}
	sr.idMu.RUnlock()

	subVersion, err := sr.registry.GetSubjectVersionByID(schemaId)
	if err != nil {
		return nil, fmt.Errorf("error obtaining subject and version for schema id:%s error:%s", strconv.Itoa(schemaId), err.Error())
	}

	if len(subVersion) == 0 {
		return nil, fmt.Errorf("error obtaining subject and version for schema id:%s due to being empty", strconv.Itoa(schemaId))
	}

	err = sr.Register(subVersion[0].Subject, subVersion[0].Version)
	if err != nil {
		return nil, err
	}

	sr.idMu.RLock()
	if cSchema, ok := sr.idSchema[schemaId]; ok {
		return cSchema, nil
	}
	sr.idMu.RUnlock()

	return nil, nil
}

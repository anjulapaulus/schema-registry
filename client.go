package schemaregistry

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type schemaPayload struct {
	Schema string `json:"schema"`
}

type SchemaResponse struct {
	ID         int         `json:"id"`
	Subject    string      `json:"subject"`
	Version    int         `json:"version"`
	SchemaType *SchemaType `json:"schemaType"`
	Schema     string      `json:"schema"`
}

type SubjectVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Client is an HTTP registry client.
type SchemaClient struct {
	client HTTPClient
	base   string
}

type clientOptions struct {
	client HTTPClient
}

type clientOps func(*clientOptions)

func WithCustomHTTPClient(client HTTPClient) clientOps {
	return func(opts *clientOptions) {
		opts.client = client
	}
}

func applyDefaultClientOptions() *clientOptions {
	ops := new(clientOptions)

	ops.client = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 90 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 3 * time.Second,
		},
	}

	return ops
}

// NewClient creates a schema registry Client with the given base url.
func NewClient(baseURL string, ops ...clientOps) (*SchemaClient, error) {
	opts := applyDefaultClientOptions()
	for _, opt := range ops {
		opt(opts)
	}
	if _, err := url.Parse(baseURL); err != nil {
		return nil, err
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	c := &SchemaClient{
		client: opts.client,
		base:   baseURL,
	}

	return c, nil
}

// GetSchema returns the schema with the given id.
func (c *SchemaClient) GetSchemaByID(id int) (string, error) {
	var payload schemaPayload
	if err := c.Request(http.MethodGet, "/schemas/ids/"+strconv.Itoa(id), nil, &payload); err != nil {
		return "", err
	}

	return payload.Schema, nil
}

// GetSubjects gets the registry subjects.
func (c *SchemaClient) GetSubjects() ([]string, error) {
	var subjects []string
	err := c.Request(http.MethodGet, "/subjects", nil, &subjects)
	if err != nil {
		return nil, err
	}

	return subjects, err
}

// GetVersions gets the schema versions for a subject.
func (c *SchemaClient) GetVersions(subject string) ([]int, error) {
	var versions []int
	err := c.Request(http.MethodGet, "/subjects/"+subject+"/versions", nil, &versions)
	if err != nil {
		return nil, err
	}

	return versions, err
}

// GetSchemaByVersion gets the schema by version.
func (c *SchemaClient) GetSchemaByVersion(subject string, version int) (SchemaResponse, error) {
	var payload SchemaResponse
	err := c.Request(http.MethodGet, "/subjects/"+subject+"/versions/"+strconv.Itoa(version), nil, &payload)
	if err != nil {
		return SchemaResponse{}, err
	}

	return payload, nil
}

// GetLatestSchema gets the latest schema for a subject.
func (c *SchemaClient) GetLatestSchema(subject string) (SchemaResponse, error) {
	var payload SchemaResponse
	err := c.Request(http.MethodGet, "/subjects/"+subject+"/versions/latest", nil, &payload)
	if err != nil {
		return SchemaResponse{}, err
	}

	return payload, nil
}

func (c *SchemaClient) GetSubjectVersionByID(schemaId int) ([]SubjectVersion, error) {
	var payload []SubjectVersion
	err := c.Request(http.MethodGet, "/schemas/ids/"+strconv.Itoa(schemaId)+"/versions", nil, &payload)
	if err != nil {
		return []SubjectVersion{}, err
	}
	return payload, nil
}

func (c *SchemaClient) Request(method, uri string, in, out interface{}) error {
	var body io.Reader
	if in != nil {
		b, _ := jsoniter.Marshal(in)
		body = bytes.NewReader(b)
	}

	req, _ := http.NewRequest(method, c.base+uri, body) // This error is not possible as we already parsed the url
	req.Header.Set("Content-Type", contentType)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		resp.Body.Close()
	}()

	if resp.StatusCode >= 400 {
		err := Error{StatusCode: resp.StatusCode}
		_ = jsoniter.NewDecoder(resp.Body).Decode(&err)
		return err
	}

	return jsoniter.NewDecoder(resp.Body).Decode(out)
}

// Error is returned by the registry when there is an error.
type Error struct {
	StatusCode int `json:"-"`

	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

// Error returns the error message.
func (e Error) Error() string {
	if e.Message != "" {
		return e.Message
	}

	return "registry error: " + strconv.Itoa(e.StatusCode)
}

package schemaregistry

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	c, err := NewClient("localhost:8080")
	if err != nil {
		t.Error("NewClient_test: failed creating client")
	}
	if c == nil {
		t.Error("NewClient_test: failed creating client, nil pointer")
	}
}

func TestNewClientWithOptions(t *testing.T) {
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 90 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 3 * time.Second,
		},
	}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("NewClient_test: failed creating client")
	}
	if c.client == nil {
		t.Error("NewClient_test: failed creating client, nil pointer")
	}
}

func TestGetSchema(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestGetSchema: failed creating client")
	}
	body := schemaPayload{
		Schema: `{
			"type": "record",
			"name": "LongList",
			"fields": [
			  {
				"name": "next",
				"type": [
				  "null",
				  "LongList"
				],
				"default": null
			  }
			]
		  }`,
	}
	out, err := json.Marshal(body)
	if err != nil {
		t.Error("TestGetSchema: failed marshalling")
	}

	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader(string(out))),
			// create the custom status code
			StatusCode: 200,
		}, nil
	}

	schema, err := c.GetSchemaByID(127)
	if err != nil {
		t.Error("TestGetSchema: failed making get schema request", err.Error())
	}

	if schema == "" {
		t.Error("TestGetSchema: schema empty", err.Error())
	}
}

func TestNoGetSchema(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestGetSchema: failed creating client")
	}

	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			Body:       ioutil.NopCloser(strings.NewReader("")),
			StatusCode: 400,
		}, nil
	}

	schema, err := c.GetSchemaByID(127)
	if err == nil {
		t.Error("TestNoGetSchema: making get schema request", err.Error())
	}

	if schema != "" {
		t.Error("TestNoGetSchema: schema empty", err.Error())
	}
}

func TestGetSubjects(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestGetSubjects: failed creating client")
	}

	body := []string{
		"com.test1",
		"com.test2",
	}

	out, err := json.Marshal(body)
	if err != nil {
		t.Error("TestGetSubjects: failed marshalling")
	}

	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader(string(out))),
			// create the custom status code
			StatusCode: 200,
		}, nil
	}

	subjects, err := c.GetSubjects()
	if err != nil {
		t.Error("TestGetSubjects: failed making get schema request", err.Error())
	}

	if len(subjects) != 2 {
		t.Error("TestGetSubjects: not all subjects")
	}
}

func TestNoGetSubjects(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestNoGetSubjects: failed creating client")
	}

	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader("")),
			// create the custom status code
			StatusCode: 400,
		}, nil
	}

	_, err = c.GetSubjects()
	if err == nil {
		t.Error("TestNoGetSubjects: making get schema request", err.Error())
	}
}

func TestGetVersions(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestGetVersions: failed creating client")
	}
	body := []int{1, 2, 3}
	out, err := json.Marshal(body)
	if err != nil {
		t.Error("TestGetVersions: failed marshalling")
	}

	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader(string(out))),
			// create the custom status code
			StatusCode: 200,
		}, nil
	}

	versions, err := c.GetVersions("com.test")
	if err != nil {
		t.Error("TestGetVersions: failed making get version request", err.Error())
	}

	if len(versions) != 3 {
		t.Error("TestGetVersions: versions empty", err.Error())
	}
}

func TestNoGetVersions(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestNoGetVersions: failed creating client")
	}
	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader("")),
			// create the custom status code
			StatusCode: 400,
		}, nil
	}

	_, err = c.GetVersions("com.test")
	if err == nil {
		t.Error("TestNoGetVersions:  making get version request", err.Error())
	}
}

func TestGetSchemaByVersion(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestGetSchemaByVersion: failed creating client")
	}
	body := SchemaResponse{
		ID:      125,
		Subject: "com.test",
		Version: 1,
		Schema: `{
			"type": "record",
			"name": "LongList",
			"fields": [
			  {
				"name": "next",
				"type": [
				  "null",
				  "LongList"
				],
				"default": null
			  }
			]
		  }`,
	}
	out, err := json.Marshal(body)
	if err != nil {
		t.Error("TestGetSchemaByVersion: failed marshalling")
	}

	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader(string(out))),
			// create the custom status code
			StatusCode: 200,
		}, nil
	}

	schemaRes, err := c.GetSchemaByVersion("com.test", 1)
	if err != nil {
		t.Error("TestGetSchemaByVersion: failed making get schema request", err.Error())
	}

	if schemaRes.Schema == "" {
		t.Error("TestGetSchemaByVersion: schema empty", err.Error())
	}
}

func TestNoGetSchemaByVersion(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestNoGetSchemaByVersion: failed creating client")
	}

	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader("")),
			// create the custom status code
			StatusCode: 400,
		}, nil
	}

	_, err = c.GetSchemaByVersion("com.test", 1)
	if err == nil {
		t.Error("TestNoGetSchemaByVersion: failed making get schema request", err.Error())
	}
}

func TestGetLatestSchema(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestGetLatestSchema: failed creating client")
	}
	body := SchemaResponse{
		ID:      125,
		Subject: "com.test",
		Version: 1,
		Schema: `{
			"type": "record",
			"name": "LongList",
			"fields": [
			  {
				"name": "next",
				"type": [
				  "null",
				  "LongList"
				],
				"default": null
			  }
			]
		  }`,
	}
	out, err := json.Marshal(body)
	if err != nil {
		t.Error("TestGetLatestSchema: failed marshalling")
	}

	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader(string(out))),
			// create the custom status code
			StatusCode: 200,
		}, nil
	}

	schemaRes, err := c.GetLatestSchema("com.test")
	if err != nil {
		t.Error("TestGetLatestSchema: failed making get schema request", err.Error())
	}

	if schemaRes.Schema == "" {
		t.Error("TestGetLatestSchema: schema empty", err.Error())
	}
}

func TestNoGetLatestSchema(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestNoGetLatestSchema: failed creating client")
	}
	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader("")),
			// create the custom status code
			StatusCode: 200,
		}, nil
	}

	_, err = c.GetLatestSchema("com.test")
	if err == nil || err.Error() == "" {
		t.Error("TestGetLatestSchema: making get schema request", err.Error())
	}
}

func TestError(t *testing.T) {
	err := Error{StatusCode: 400}
	if err.Error() == "" {
		t.Error("TestError: Empty Error")
	}
	err1 := Error{Message: "Test Message", StatusCode: 400}
	if err1.Error() == "" {
		t.Error("TestError: Empty Error")
	}
}

func TestRequest(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestNoGetLatestSchema: failed creating client")
	}
	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader("")),
			// create the custom status code
			StatusCode: 400,
		}, nil
	}

	c.Request(http.MethodGet, "", "", nil)
}

func TestDoRequest(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestNoGetLatestSchema: failed creating client")
	}
	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{}, errors.New("Client do error")
	}

	c.Request(http.MethodGet, "", "", nil)
}

func TestGetSubjectVersionByID(t *testing.T) {
	client := &HTTPClientMock{}
	c, err := NewClient("localhost:8080", WithCustomHTTPClient(client))
	if err != nil {
		t.Error("TestGetSubjectVersionByID: failed creating client")
	}
	body := SubjectVersion{
		Subject: "com.test",
		Version: 1,
	}
	out, err := json.Marshal(body)
	if err != nil {
		t.Error("TestGetSubjectVersionByID: failed marshalling")
	}

	client.DoFunc = func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			// create the custom body
			Body: ioutil.NopCloser(strings.NewReader(string(out))),
			// create the custom status code
			StatusCode: 200,
		}, nil
	}

	_, err = c.GetSubjectVersionByID(200)
	if err == nil || err.Error() == "" {
		t.Error("TestGetSubjectVersionByID: making get schema request", err.Error())
	}
}

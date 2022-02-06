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
		Schema: "{\"type\":\"record\",\"name\":\"BankStatusChanged\",\"namespace\":\"com.pickme.events.payment\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"body\",\"type\":{\"type\":\"record\",\"name\":\"Body\",\"namespace\":\"com.pickme.events.payment.bank_status_changed\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"bank\",\"type\":\"int\"},{\"name\":\"transaction_reference_id\",\"type\":\"string\"},{\"name\":\"payment_type\",\"type\":\"int\"},{\"name\":\"payment_type_reference_id\",\"type\":\"long\"},{\"name\":\"status\",\"type\":\"int\"},{\"name\":\"updated_datetime\",\"type\":\"long\"},{\"name\":\"created_datetime\",\"type\":\"long\"}]}},{\"name\":\"created_at\",\"type\":\"long\"},{\"name\":\"expiry\",\"type\":\"long\",\"default\":0},{\"name\":\"version\",\"type\":\"int\",\"default\":0},{\"name\":\"trace_info\",\"type\":{\"type\":\"record\",\"name\":\"TraceInfo\",\"fields\":[{\"name\":\"trace_id\",\"type\":{\"type\":\"record\",\"name\":\"TraceId\",\"fields\":[{\"name\":\"high\",\"type\":\"long\",\"default\":0},{\"name\":\"low\",\"type\":\"long\",\"default\":0}]}},{\"name\":\"span_id\",\"type\":\"long\",\"default\":0},{\"name\":\"parent_id\",\"type\":\"long\",\"default\":0},{\"name\":\"sampled\",\"type\":\"boolean\",\"default\":false}]}}]}",
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
		"com.pickme.events.job.ArrivedAtPickup",
		"shuttle_passenger_alight-value",
		"com.pickme.events.job.PreptimeExtendFailed",
		"driver_services.payment_events.complete_payment",
		"com.pickme.events.auth.TokenRevoked",
		"heat_changed-value",
		"com.pickme.events.driver.DriverVehicleAssigned",
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

	if len(subjects) != 7 {
		t.Error("TestGetSubjects: not all subjects", err.Error())
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

	versions, err := c.GetVersions("com.pickme.events.driver.DriverVehicleDowngraded")
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

	_, err = c.GetVersions("com.pickme.events.driver.DriverVehicleDowngraded")
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
		Subject: "com.pickme.events.driver.DriverVehicleDowngraded",
		Version: 1,
		Schema:  "{\"type\":\"record\",\"name\":\"DriverVehicleDowngraded\",\"namespace\":\"com.pickme.events.driver\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"body\",\"type\":{\"type\":\"record\",\"name\":\"Body\",\"namespace\":\"com.pickme.events.driver.driver_vehicle_downgraded\",\"fields\":[{\"name\":\"driver_id\",\"type\":\"int\"},{\"name\":\"vehicle_models\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}},{\"name\":\"created_at\",\"type\":\"long\"},{\"name\":\"expiry\",\"type\":\"long\",\"default\":0},{\"name\":\"version\",\"type\":\"int\",\"default\":0},{\"name\":\"trace_info\",\"type\":{\"type\":\"record\",\"name\":\"TraceInfo\",\"fields\":[{\"name\":\"trace_id\",\"type\":{\"type\":\"record\",\"name\":\"TraceId\",\"fields\":[{\"name\":\"high\",\"type\":\"long\",\"default\":0},{\"name\":\"low\",\"type\":\"long\",\"default\":0}]}},{\"name\":\"span_id\",\"type\":\"long\",\"default\":0},{\"name\":\"parent_id\",\"type\":\"long\",\"default\":0},{\"name\":\"sampled\",\"type\":\"boolean\",\"default\":false}]}}]}",
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

	schemaRes, err := c.GetSchemaByVersion("com.pickme.events.driver.DriverVehicleDowngraded", 1)
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

	_, err = c.GetSchemaByVersion("com.pickme.events.driver.DriverVehicleDowngraded", 1)
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
		Subject: "com.pickme.events.driver.DriverVehicleDowngraded",
		Version: 1,
		Schema:  "{\"type\":\"record\",\"name\":\"DriverVehicleDowngraded\",\"namespace\":\"com.pickme.events.driver\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"body\",\"type\":{\"type\":\"record\",\"name\":\"Body\",\"namespace\":\"com.pickme.events.driver.driver_vehicle_downgraded\",\"fields\":[{\"name\":\"driver_id\",\"type\":\"int\"},{\"name\":\"vehicle_models\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}},{\"name\":\"created_at\",\"type\":\"long\"},{\"name\":\"expiry\",\"type\":\"long\",\"default\":0},{\"name\":\"version\",\"type\":\"int\",\"default\":0},{\"name\":\"trace_info\",\"type\":{\"type\":\"record\",\"name\":\"TraceInfo\",\"fields\":[{\"name\":\"trace_id\",\"type\":{\"type\":\"record\",\"name\":\"TraceId\",\"fields\":[{\"name\":\"high\",\"type\":\"long\",\"default\":0},{\"name\":\"low\",\"type\":\"long\",\"default\":0}]}},{\"name\":\"span_id\",\"type\":\"long\",\"default\":0},{\"name\":\"parent_id\",\"type\":\"long\",\"default\":0},{\"name\":\"sampled\",\"type\":\"boolean\",\"default\":false}]}}]}",
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

	schemaRes, err := c.GetLatestSchema("com.pickme.events.driver.DriverVehicleDowngraded")
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

	_, err = c.GetLatestSchema("com.pickme.events.driver.DriverVehicleDowngraded")
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

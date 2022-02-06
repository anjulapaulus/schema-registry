package schemaregistry

import (
	"net"
	"net/http"
	"testing"
	"time"
)

func TestNewSchemaRegistryEmptyURL(t *testing.T) {
	_, err := NewRegistry("")
	if err == nil {
		t.Error("TestNewSchemaRegistryEmptyURL: empty url not handled")
	}
}

func TestNewSchemaRegistry(t *testing.T) {
	reg, err := NewRegistry("localhost:8080")
	if err != nil {
		t.Error("TestNewSchemaRegistry: ", err)
	}
	if reg == nil {
		t.Error("TestNewSchemaRegistry: registry nil", reg)
	}
}

func TestNewSchemaRegistryWithCustomOptions_HTTPClient(t *testing.T) {
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
	reg, err := NewRegistry("localhost:8080", WithHTTPClient(client))
	if err != nil {
		t.Error("TestNewSchemaRegistryWithCustomOptions_HTTPClient: ", err)
	}
	if reg.registry.client == nil {
		t.Error("TestNewSchemaRegistryWithCustomOptions_HTTPClient: registry nil", reg)
	}
}

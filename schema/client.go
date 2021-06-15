package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
)

type RegistryClient struct {
	url                     string
	httpClient              *http.Client
	enableCache             bool
	enableCacheLock         sync.RWMutex
	enableCodecCreation     bool
	enableCodecCreationLock sync.RWMutex
	schemaIDCache           map[int]*Schema
	schemaIDCacheLock       sync.RWMutex
	schemaSubjectCache      map[string]SubjectSchema
	schemaSubjectCacheLock  sync.RWMutex
	subjectIDCache			map[string][]int
	subjectIDCacheLock		sync.RWMutex
}

type Schema struct {
	ID      int
	Schema  string
	Version int
	Codec   *goavro.Codec
}

type SubjectSchema []*Schema

type schema struct {
	ID      int    `json:"id"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

type version struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

const (
	contentType = "application/vnd.schemaregistry.v1+json"

	schemaByID      = "/schemas/ids/%d"
	schemaTypes     = "/schemas/types/"
	schemaIDVersion = "/schemas/ids/%d/versions"

	allSubjects            = "/subjects"
	schemaSubjectVersion   = "/subjects/%s/versions"
	deleteSubject          = "/subjects/%s?permanent=%t"
	schemaBySubjectVersion = "/subjects/%s/versions/%d"
	latestSchema           = "/subjects/%s/versions/latest"
	schemaReferencedBy     = "/subjects/%s/versions/%s/referencedby"

	compatibility = "/compatibility/subjects/%s/versions/%v"


)

func NewSchemaRegistryClient(url string) *RegistryClient {
	return &RegistryClient{
		url:                 url,
		httpClient:          &http.Client{Timeout: 5 * time.Second},
		enableCache:         true,
		enableCodecCreation: false,
		schemaIDCache:       make(map[int]*Schema),
		schemaSubjectCache:  make(map[string]SubjectSchema),
		subjectIDCache:		 make(map[string][]int),
	}
}

func (sr *RegistryClient) GetSchemaByID(id int) (*Schema, error) {
	// If schema exists in cache, return schema for ID
	if sr.isCachingAvailable() {
		sr.schemaIDCacheLock.RLock()
		if schema, ok := sr.schemaIDCache[id]; ok {
			sr.schemaIDCacheLock.RUnlock()
			return schema, nil
		}
	}

	body, err := sr.request("GET", fmt.Sprintf(schemaByID, id), nil)
	if err != nil {
		return nil, err
	}
	var schemaResp = new(schema)
	err = json.Unmarshal(body, &schemaResp)
	if err != nil {
		return nil, err
	}

	versionBody, err := sr.request("GET", fmt.Sprintf(schemaIDVersion, id), nil)
	if err != nil {
		return nil, err
	}

	var result []version
	err = json.Unmarshal(versionBody, &result)
	if err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if sr.enableCodecCreation {
		codec, err = goavro.NewCodec(schemaResp.Schema)
		if err != nil {
			return nil, err
		}
	}
	var schema = &Schema{
		ID:      id,
		Schema:  schemaResp.Schema,
		Version: result[0].Version,
		Codec:   codec,
	}

	if sr.enableCache {
		//id - schema cache
		sr.schemaIDCacheLock.Lock()
		sr.schemaIDCache[id] = schema
		sr.schemaIDCacheLock.Unlock()

		if schemas, ok := sr.schemaSubjectCache[schemaResp.Subject]; ok {
			sr.schemaSubjectCacheLock.Lock()
			sr.schemaSubjectCache[schemaResp.Subject] = append(schemas, schema)
			sr.schemaSubjectCacheLock.Unlock()
		} else {
			var array SubjectSchema
			array = append(array, schema)
			sr.schemaSubjectCacheLock.Lock()
			sr.schemaSubjectCache[schemaResp.Subject] = array
			sr.schemaSubjectCacheLock.Unlock()
		}
		// subject-ids mapping
		if schemaIds, ok := sr.subjectIDCache[schemaResp.Subject]; ok {
			sr.subjectIDCacheLock.Lock()
			sr.subjectIDCache[schemaResp.Subject] = append(schemaIds, id)
			sr.subjectIDCacheLock.Unlock()
		}else{
			var idArray []int
			idArray = append(idArray, id)
			sr.subjectIDCacheLock.Lock()
			sr.subjectIDCache[schemaResp.Subject] = idArray
			sr.subjectIDCacheLock.Unlock()
		}

	}
	return schema, nil
}

func (sr *RegistryClient) GetSchemaTypes() (typesSchemas []string, err error) {
	body, err := sr.request("GET", schemaTypes, nil)
	if err != nil {
		return
	}

	err = json.Unmarshal(body, &typesSchemas)
	if err != nil {
		return
	}
	return
}

func (sr *RegistryClient) GetSchemaIDVersions(id int) (int, error) {
	if sr.isCachingAvailable() {
		sr.schemaIDCacheLock.RLock()
		if schema, ok := sr.schemaIDCache[id]; ok {
			sr.schemaIDCacheLock.RUnlock()
			return schema.Version, nil
		}
	}
	body, err := sr.request("GET", fmt.Sprintf(schemaIDVersion, id), nil)
	if err != nil {
		return 0, err
	}
	var result []version
	err = json.Unmarshal(body, &result)
	if err != nil {
		return 0, err
	}
	return result[0].Version, nil
}

func (sr *RegistryClient) GetAllSubjects() (subjects []string, err error) {

	body, err := sr.request("GET", allSubjects, nil)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &subjects)
	if err != nil {
		return nil, err
	}
	return

}

func (sr *RegistryClient) GetSubjectVersions(subject string) ([]int, error) {
	if sr.isCachingAvailable() {
		sr.schemaSubjectCacheLock.RLock()
		schema := sr.schemaSubjectCache[subject]
		sr.schemaSubjectCacheLock.RUnlock()

		if schema != nil {
			var versionArr []int
			for _, s := range schema {
				versionArr = append(versionArr, s.Version)
			}
			return versionArr, nil
		}
	}
	body, err := sr.request("GET", fmt.Sprintf(schemaSubjectVersion, subject), nil)
	if err != nil {
		return nil, err
	}
	var version []int
	err = json.Unmarshal(body, &version)
	if err != nil {
		return nil, err
	}
	return version, nil
}

func (sr *RegistryClient) DeleteSubject(subject string, hardDelete bool) ([]int, error) {
	if sr.isCachingAvailable() {
		sr.schemaSubjectCacheLock.RLock()
		delete(sr.schemaSubjectCache, subject)
		sr.schemaSubjectCacheLock.RUnlock()
	}
	body, err := sr.request("DELETE", fmt.Sprintf(deleteSubject, subject, hardDelete), nil)
	if err != nil {
		return nil, err
	}

	var versions []int
	err = json.Unmarshal(body, &versions)
	if err != nil {
		return nil, err
	}
	return versions, nil
}

func (sr *RegistryClient) GetSchemaForVersion(subject string, version int) (*Schema, error) {
	if sr.isCachingAvailable() {
		sr.schemaSubjectCacheLock.RLock()
		schema := sr.schemaSubjectCache[subject]
		sr.schemaSubjectCacheLock.RUnlock()
		if schema != nil {
			for _, s := range schema {
				if s.Version == version {
					return s, nil
				}
			}
		}
	}

	body, err := sr.request("GET", fmt.Sprintf(schemaBySubjectVersion, subject, version), nil)
	if err != nil {
		return nil, err
	}

	var schemaResp = new(schema)
	err = json.Unmarshal(body, &schemaResp)
	if err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if sr.enableCodecCreation {
		codec, err = goavro.NewCodec(schemaResp.Schema)
		if err != nil {
			return nil, err
		}
	}
	var schema = &Schema{
		ID:      schemaResp.ID,
		Schema:  schemaResp.Schema,
		Version: version,
		Codec:   codec,
	}

	var array SubjectSchema
	array = append(array, schema)
	if sr.enableCache {
		sr.schemaSubjectCacheLock.Lock()
		sr.schemaSubjectCache[subject] = array
		sr.schemaSubjectCacheLock.Unlock()
	}

	return schema, nil
}

func (sr *RegistryClient) GetLatestSchema(subject string) (*Schema, error) {
	if sr.isCachingAvailable() {
		sr.schemaSubjectCacheLock.RLock()
		schema := sr.schemaSubjectCache[subject]
		sr.schemaSubjectCacheLock.RUnlock()
		if schema != nil {
			schema := (schema)[len(schema)-1]
			return schema, nil
		}
	}
	body, err := sr.request("GET", fmt.Sprintf(latestSchema, subject), nil)
	if err != nil {
		return nil, err
	}

	var schemaResp = new(schema)
	err = json.Unmarshal(body, &schemaResp)
	if err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if sr.enableCodecCreation {
		codec, err = goavro.NewCodec(schemaResp.Schema)
		if err != nil {
			return nil, err
		}
	}
	var schema = &Schema{
		ID:      schemaResp.ID,
		Schema:  schemaResp.Schema,
		Version: schemaResp.Version,
		Codec:   codec,
	}

	var array SubjectSchema
	array = append(array, schema)
	if sr.enableCache {
		sr.schemaSubjectCacheLock.Lock()
		sr.schemaSubjectCache[subject] = array
		sr.schemaSubjectCacheLock.Unlock()
	}

	return schema, nil
}
func (sr *RegistryClient) referencedBy(subject string, version string) ([]int, error) {
	body, err := sr.request("GET", fmt.Sprintf(schemaReferencedBy, subject, version), nil)
	if err != nil {
		return nil, err
	}
	var schemaIDs []int
	err = json.Unmarshal(body, &schemaIDs)
	if err != nil {
		return nil, err
	}
	return schemaIDs, nil
}
func (sr *RegistryClient) GetSchemaReferencedBy(subject string, version int) ([]int, error) {
	schemaIDs, err := sr.referencedBy(subject, strconv.Itoa(version))
	if err != nil {
		return nil, err
	}
	return schemaIDs, nil
}

func (sr *RegistryClient) GetLatestSchemaReferencedBy(subject string) ([]int, error) {
	schemaIDs, err := sr.referencedBy(subject, "latest")
	if err != nil {
		return nil, err
	}
	return schemaIDs, nil
}

func (sr *RegistryClient) isCachingAvailable() bool {
	sr.enableCacheLock.RLock()
	defer sr.enableCacheLock.RUnlock()
	return sr.enableCache
}

func (sr *RegistryClient) request(method, uri string, payload io.Reader) ([]byte, error) {
	url := fmt.Sprintf("%s%s", sr.url, uri)
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", contentType)

	resp, err := sr.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, createError(resp)
	}

	return ioutil.ReadAll(resp.Body)
}

func createError(resp *http.Response) error {
	decoder := json.NewDecoder(resp.Body)
	var errorResp struct {
		ErrorCode int    `json:"error_code"`
		Message   string `json:"message"`
	}
	err := decoder.Decode(&errorResp)
	if err == nil {
		return fmt.Errorf("%s: %s", resp.Status, errorResp.Message)
	}
	return fmt.Errorf("%s", resp.Status)
}

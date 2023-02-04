package schemaregistry

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"gitlab.test.igdcs.com/finops/nextgen/sandbox/kafka-poc/kafkalib/connection"

	gojson "github.com/goccy/go-json"
)

//go:generate mockgen -source=./schema-registry.go -destination=mocks-schema-registry.go -package=schemaregistry
type Logger interface {
	Debug(string)
	Debugf(string, ...interface{})
	Info(string)
	Infof(string, ...interface{})
	Error(string)
	Errorf(string, ...interface{})
}

type (
	// SchemaRegistry is a schema registry wrapper.
	// It handles connection with a schema registry, allows registering and configuring schemas.
	// For a better performance SchemaRegistry caches schemas and decoders.
	SchemaRegistry struct {
		baseURL           string
		timeout           time.Duration
		httpClient        *http.Client
		logger            Logger
		connectionTimeout time.Duration
	}

	Config struct {
		Host               string        `json:"host" yaml:"host" envconfig:"HOST"`
		Timeout            time.Duration `json:"timeout" yaml:"timeout" envconfig:"TIMEOUT"`
		InsecureSkipVerify bool          `json:"insecure_skip_verify" yaml:"insecure-skip-verify" envconfig:"INSECURITY_SKIP_VERIFY"`
		ConnectionTimeout  time.Duration `json:"connection_timeout" yaml:"connection-timeout" envconfig:"CONNECTION_TIMEOUT"`
	}

	CompatibilityType string

	LogLevel string
)

const (
	defaultTimeout           = 15 * time.Second
	defaultConnectionTimeout = 60 * time.Second
)

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ErrNotFound     = Error("not found")
	ErrUnknownError = Error("unknown error")

	UnmarshallingErrMessage = "unmarshalling response: %w"
)

// NewSchemaRegistry creates new schema registry client and returns and builds SchemaRegistry.
func NewSchemaRegistry(config Config, logger Logger) (*SchemaRegistry, error) {
	if config.Host == "" {
		return nil, errors.New("empty URL for schema registry")
	}

	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}

	if config.ConnectionTimeout == 0 {
		config.Timeout = defaultConnectionTimeout
	}

	var transport http.RoundTripper
	if config.InsecureSkipVerify {
		transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // yes, it's insecure
		}
	}

	httpClient := http.Client{
		Timeout:   config.Timeout,
		Transport: transport,
	}

	baseURL, err := formatBaseURL(config.Host)
	if err != nil {
		return nil, fmt.Errorf("bad host: %w", err)
	}

	return &SchemaRegistry{
		baseURL:           baseURL,
		timeout:           config.Timeout,
		httpClient:        &httpClient,
		logger:            logger,
		connectionTimeout: config.ConnectionTimeout,
	}, nil
}

// Ping checks connection with a schema registry.
func (r *SchemaRegistry) Ping() error {
	pingHost, err := formatPingHost(r.baseURL)
	if err != nil {
		return fmt.Errorf("bad ping host: %w", err)
	}

	r.logger.Debugf("ping schema registry [%v]", pingHost)

	if _, err = net.DialTimeout("tcp", pingHost, r.timeout); err != nil {
		return fmt.Errorf("dialing [%s]: %w", pingHost, err)
	}

	return nil
}

// CheckConnection pings schema registry host until getting success or connectionTimeout is reached.
func (r *SchemaRegistry) CheckConnection(ctx context.Context) error {
	if err := connection.CheckConnections(ctx, r.Ping, r.connectionTimeout, r.logger); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	return nil
}

const (
	subjectsPath = "subjects"
	subjectPath  = subjectsPath + "/%s"
	schemaPath   = "schemas/ids/%d"
)

type (
	idOnlyJSON struct {
		ID int `json:"id"`
	}
)

// RegisterSchema registers a schema.
// The returned identifier should be used to retrieve
// this schema from the schemas resource and is different from
// the schema’s version which is associated with that name.
//
// POST /subjects/(string: subject)/versions .
func (r *SchemaRegistry) RegisterSchema(ctx context.Context, topic string, schema []byte) (int, error) {
	if len(topic) == 0 {
		return 0, errors.New("empty topic")
	}

	if len(schema) == 0 {
		return 0, errors.New("empty schema")
	}

	subject := getSubject(topic)

	schemaContainer := createJSONSchemaContainer(schema)

	containerBuffer, err := gojson.Marshal(schemaContainer)
	if err != nil {
		return 0, fmt.Errorf("marshaling schema container: %w", err)
	}

	path := fmt.Sprintf(subjectPath+"/versions", subject)
	respBody, err := r.do(ctx, http.MethodPost, path, contentTypeJSON, containerBuffer)
	if err != nil {
		return 0, err
	}

	var res idOnlyJSON
	if err := gojson.Unmarshal(respBody, &res); err != nil {
		return 0, fmt.Errorf(UnmarshallingErrMessage, err)
	}

	return res.ID, nil
}

// GetSchemaByID returns the Avro schema string identified by the id.
//
// {id} (int) – the globally unique identifier of the schema.
//
// GET /schemas/ids/{int: id} .
func (r *SchemaRegistry) GetSchemaByID(ctx context.Context, subjectID int) (string, error) {
	path := fmt.Sprintf(schemaPath, subjectID)
	respBody, err := r.do(ctx, http.MethodGet, path, "", nil)

	if IsSchemaNotFound(err) {
		return "", ErrNotFound
	}

	if err != nil {
		return "", err
	}

	var res schemaOnlyJSON
	if err := gojson.Unmarshal(respBody, &res); err != nil {
		return "0", fmt.Errorf(UnmarshallingErrMessage, err)
	}

	return res.Schema, nil
}

// SchemaLatestVersion is the only one valid string for the "versionID", it's the "latest" version string and it's used on `GetLatestSchema`.
const SchemaLatestVersion = "latest"

const (
	BadSchemaIntVersionError    = `client: %v string is not a valid value for the versionID input parameter [versionID == "latest"]`
	BadSchemaStringVersionError = `client: %v integer is not a valid value for the versionID input parameter [ versionID > 0 && versionID <= 2^31-1]`
)

func checkSchemaVersionID(versionID interface{}) error {
	if versionID == nil {
		return errRequired(`versionID (string "latest" or int)`)
	}

	switch vID := versionID.(type) {
	case string:
		if vID != SchemaLatestVersion {
			return fmt.Errorf(BadSchemaIntVersionError, versionID)
		}

	case int:
		if vID <= 0 || vID > 2^31-1 { // it's the max of int32, math.MaxInt32 already but do that check.
			return fmt.Errorf(BadSchemaStringVersionError, versionID)
		}
	}

	return nil
}

// subject is {topic}-value
//
// GET /subjects/(string: subject)/versions/(versionId: "latest" | int)
//
// version (versionId [string "latest" or 1,2^31-1]) – Version of the schema to be returned.
// Valid values for versionId are between [1,2^31-1] or the string “latest”.
// The string “latest” refers to the last registered schema under the specified subject.
// Note that there may be a new latest schema that gets registered right after this request is served.
//
// It's not safe to use just an interface to the high-level API, therefore we split this method
// to two, one which will retrieve the latest versioned schema and the other which will accept
// the version as integer and it will retrieve by a specific version.
//
// See `GetLatestSchema` and `GetSchemaAtVersion` instead.
func (r *SchemaRegistry) getTopicSchemaAtVersion(ctx context.Context, topic string, versionID interface{}) (*Schema, error) {
	if topic == "" {
		return nil, errRequired("subject")
	}

	if err := checkSchemaVersionID(versionID); err != nil {
		return nil, err
	}

	subject := getSubject(topic)

	path := fmt.Sprintf(subjectPath+"/versions/%v", subject, versionID)
	respBody, respErr := r.do(ctx, http.MethodGet, path, "", nil)
	if respErr != nil {
		if IsSubjectNotFound(respErr) {
			return nil, ErrNotFound
		}

		return nil, respErr
	}

	res := Schema{}
	if err := gojson.Unmarshal(respBody, &res); err != nil {
		return nil, fmt.Errorf(UnmarshallingErrMessage, err)
	}

	return &res, nil
}

// GetLatestSchema returns the latest version of a schema.
// See `GetSchemaAtVersion` to retrieve a subject schema by a specific version.
//
// {subject} is {topic}-value
//
// GET /subjects/(string: subject)/versions/latest .
func (r *SchemaRegistry) GetLatestSchema(ctx context.Context, topic string) (*Schema, error) {
	return r.getTopicSchemaAtVersion(ctx, topic, SchemaLatestVersion)
}

// GetSchemaByTopic returns the schema for a particular subject and version.
//
// {subject} is {topic}-value
//
// GET /subjects/(string: subject)/versions/[versionId] .
func (r *SchemaRegistry) GetSchemaByTopic(ctx context.Context, topic string, versionID int) (*Schema, error) {
	return r.getTopicSchemaAtVersion(ctx, topic, versionID)
}

// subject (string) – Name of the subject
// POST /compatibility/subjects/(string: subject)/versions/(versionId: "latest" | int)
//
// version (versionId [string "latest" or 1,2^31-1]) – Version of the schema to be returned.
// Valid values for versionId are between [1,2^31-1] or the string “latest”.
// The string “latest” refers to the last registered schema under the specified subject.
// Note that there may be a new latest schema that gets registered right after this request is served.
//
// It's not safe to use just an interface to the high-level API, therefore we split this method
// to two, one which will retrieve the latest versioned schema and the other which will accept
// the version as integer and it will retrieve by a specific version.
//
// See `IsSchemaCompatible` and `IsLatestSchemaCompatible` instead.
func (r *SchemaRegistry) isSchemaCompatibleAtVersion(ctx context.Context, topic, schema string, versionID interface{}) (bool, error) {
	if len(topic) == 0 {
		return false, errors.New("empty subject")
	}
	if len(schema) == 0 {
		return false, errors.New("empty schema")
	}

	subject := getSubject(topic)

	if err := checkSchemaVersionID(versionID); err != nil {
		return false, err
	}

	schemaContainer := schemaContainer{
		SchemaType: "JSON",
		Schema:     schema,
	}

	send, err := gojson.Marshal(schemaContainer)
	if err != nil {
		return false, fmt.Errorf("marshaling schema container: %w", err)
	}

	// # Test input schema against a particular version of a subject’s schema for compatibility
	// POST /compatibility/subjects/(string: subject)/versions/(versionId: "latest" | int)
	path := fmt.Sprintf("compatibility/"+subjectPath+"/versions/%v", subject, versionID)
	respBody, err := r.do(ctx, http.MethodPost, path, contentTypeSchemaJSON, send)
	if err != nil {
		return false, err
	}

	var res isCompatibleJSON
	if err := gojson.Unmarshal(respBody, &res); err != nil {
		return false, fmt.Errorf(UnmarshallingErrMessage, err)
	}

	return res.IsCompatible, nil
}

// IsSchemaCompatible tests compatibility with a specific version of a subject's schema.
func (r *SchemaRegistry) IsSchemaCompatible(ctx context.Context, topic, schema string, versionID int) (bool, error) {
	return r.isSchemaCompatibleAtVersion(ctx, topic, schema, versionID)
}

// IsLatestSchemaCompatible tests compatibility with the latest version of a subject's schema.
func (r *SchemaRegistry) IsLatestSchemaCompatible(ctx context.Context, topic, schema string) (bool, error) {
	return r.isSchemaCompatibleAtVersion(ctx, topic, schema, SchemaLatestVersion)
}

package schemaregistry

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	gojson "github.com/goccy/go-json"
)

const (
	contentTypeHeaderKey = "Content-Type"
	contentTypeJSON      = "application/json"

	acceptHeaderKey          = "Accept"
	acceptEncodingHeaderKey  = "Accept-Encoding"
	contentEncodingHeaderKey = "Content-Encoding"
	gzipEncodingHeaderValue  = "gzip"

	schemaAPIVersion      = "v1"
	contentTypeSchemaJSON = "application/vnd.schemaregistry." + schemaAPIVersion + "+json"
)

// ResourceError is being fired from all API calls when an error code is received.
type ResourceError struct {
	ErrorCode int    `json:"error_code"`
	Method    string `json:"method,omitempty"`
	URI       string `json:"uri,omitempty"`
	Message   string `json:"message,omitempty"`
}

func (err ResourceError) Error() string {
	return fmt.Sprintf("client: (%s: %s) failed with error code %d%s",
		err.Method, err.URI, err.ErrorCode, err.Message)
}

func newResourceError(errCode int, uri, method, body string) ResourceError {
	unescapedURI, _ := url.QueryUnescape(uri)

	return ResourceError{
		ErrorCode: errCode,
		URI:       unescapedURI,
		Method:    method,
		Message:   body,
	}
}

// These numbers are used by the schema registry to communicate errors.
const (
	subjectNotFoundCode = 40401
	schemaNotFoundCode  = 40403
)

// IsSubjectNotFound checks the returned error to see if it is kind of a subject not found  error code.
func IsSubjectNotFound(err error) bool {
	if err == nil {
		return false
	}

	var resErr ResourceError
	if errors.As(err, &resErr) {
		return resErr.ErrorCode == subjectNotFoundCode
	}

	return false
}

// IsSchemaNotFound checks the returned error to see if it is kind of a schema not found  error code.
func IsSchemaNotFound(err error) bool {
	if err == nil {
		return false
	}

	var resErr ResourceError
	if errors.As(err, &resErr) {
		return resErr.ErrorCode == schemaNotFoundCode
	}

	return false
}

// isOK is called inside the `Client#do` and it closes the body reader if no accessible.
func isOK(resp *http.Response) bool {
	return !(resp.StatusCode < 200 || resp.StatusCode >= 300)
}

var noOpBuffer = new(bytes.Buffer)

func acquireBuffer(b []byte) *bytes.Buffer {
	if len(b) > 0 {
		return bytes.NewBuffer(b)
	}

	return noOpBuffer
}

func (r *SchemaRegistry) do(ctx context.Context, method, path, contentType string, send []byte) ([]byte, error) {
	if path[0] == '/' {
		path = path[1:]
	}

	uri := r.baseURL + "/" + path

	req, err := http.NewRequestWithContext(ctx, method, uri, acquireBuffer(send))
	if err != nil {
		return nil, fmt.Errorf("creating http request: %w", err)
	}

	// set the content type if any.
	if contentType != "" {
		req.Header.Set(contentTypeHeaderKey, contentType)
	}

	// response accept gziped content.
	req.Header.Add(acceptEncodingHeaderKey, gzipEncodingHeaderValue)
	req.Header.Add(acceptHeaderKey, contentTypeSchemaJSON+", application/vnd.schemaregistry+json, application/json")
	req.Header.Add(acceptHeaderKey, contentTypeJSON)

	// send the request and check the response for any connection & authorization errors here.
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if isOK(resp) {
		body, err := readResponseBody(resp)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}

		return body, nil
	}

	// bad response
	respContentType := resp.Header.Get(contentTypeHeaderKey)

	errBody, err := readResponseBody(resp)
	if err != nil {
		return nil, ErrUnknownError
	}

	if strings.Contains(respContentType, "json") {
		var resErr ResourceError
		if err := gojson.Unmarshal(errBody, &resErr); err == nil {
			return nil, resErr
		}
	}

	// if error is not json
	return nil, newResourceError(resp.StatusCode, uri, method, string(errBody))
}

type gzipReadCloser struct {
	respReader io.ReadCloser
	gzipReader io.ReadCloser
}

func (rc *gzipReadCloser) Close() error {
	if rc.gzipReader != nil {
		defer func() { _ = rc.gzipReader.Close() }()
	}

	if err := rc.respReader.Close(); err != nil {
		return fmt.Errorf("closing gzip reader: %w", err)
	}

	return nil
}

func (rc *gzipReadCloser) Read(buf []byte) (int, error) {
	rdr := rc.respReader

	if rc.gzipReader != nil {
		rdr = rc.gzipReader
	}

	return rdr.Read(buf) //nolint:wrapcheck // nothing to add
}

func acquireResponseBodyStream(resp *http.Response) (io.ReadCloser, error) {
	// check for gzip and read it, the right way.

	bodyReader := resp.Body

	if encoding := resp.Header.Get(contentEncodingHeaderKey); encoding == gzipEncodingHeaderValue {
		reader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("client: failed to read gzip compressed content, trace: %v", err)
		}

		// we wrap the gzipReader and the underline response reader
		// so a call of .Close() can close both of them with the correct order when finish reading, the caller decides.
		// Must close manually using defer on the callers before the `readResponseBody` call,
		// note that the `readJSON` can decide correctly by itself.
		return &gzipReadCloser{
			respReader: resp.Body,
			gzipReader: reader,
		}, nil
	}

	// return the stream reader.
	return bodyReader, nil
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	reader, err := acquireResponseBodyStream(resp)
	if err != nil {
		return nil, fmt.Errorf("acquiring response body: %w", err)
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("closing response: %w", err)
	}

	// return the body.
	return body, nil
}

var errRequired = func(field string) error {
	return fmt.Errorf("client: %s is required", field)
}

type (
	schemaContainer struct {
		SchemaType string `json:"schemaType"`
		Schema     string `json:"schema"`
	}

	isCompatibleJSON struct {
		IsCompatible bool `json:"is_compatible"`
	}

	schemaOnlyJSON struct {
		Schema string `json:"schema"`
	}

	// Schema describes a schema, look `GetSchema` for more.
	Schema struct {
		// Schema is the schema string.
		Schema string `json:"schema"`
		// Subject where the schema is registered for.
		Subject string `json:"subject"`
		// Version of the returned schema.
		Version int `json:"version"`
		ID      int `json:"id,omitempty"`
	}
)

func createJSONSchemaContainer(schema []byte) schemaContainer {
	container := schemaContainer{
		SchemaType: "JSON",
		Schema:     string(schema),
	}

	return container
}

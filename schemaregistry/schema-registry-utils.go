package schemaregistry

import (
	"errors"
	"fmt"
	"strings"
)

const (
	schemaSeparator = "://"
	schemaHTTP      = "http"
	schemaHTTPS     = "https"
)

func formatPingHost(srcURL string) (string, error) {
	if len(srcURL) == 0 {
		return "", errors.New("empty URL")
	}

	resURL := srcURL

	// Removing everything after the question mark
	questMarkIdx := strings.LastIndexByte(resURL, '?')
	if questMarkIdx >= 0 {
		resURL = resURL[:questMarkIdx]
	}

	// Removing the last slash
	if resURL[len(resURL)-1] == '/' {
		resURL = resURL[:len(resURL)-1]
	}

	// Getting schema
	schemaIdx := strings.Index(resURL, schemaSeparator)
	schema := schemaHTTP
	if schemaIdx >= 0 {
		schema = resURL[:schemaIdx]
		resURL = resURL[schemaIdx+len(schemaSeparator):]
	}

	// Removing credentials
	credIdx := strings.Index(resURL, "@")
	if credIdx >= 0 {
		resURL = resURL[credIdx+1:]
	}

	// Getting port
	port := "80"
	hasPort := false

	portIdx := strings.LastIndexByte(resURL, ':')
	if portIdx >= 0 {
		port = resURL[portIdx+1:]
		resURL = resURL[:portIdx]
		hasPort = true
	}

	// standard port for https
	if !hasPort && schema == schemaHTTPS {
		port = "443"
	}

	if len(resURL) == 0 {
		return "", fmt.Errorf("resolving [%v] to empty address", srcURL)
	}

	return resURL + ":" + port, nil
}

func formatBaseURL(srcURL string) (string, error) {
	if len(srcURL) == 0 {
		return "", errors.New("empty URL")
	}

	resURL := srcURL

	// Removing everything after the question mark
	questMarkIdx := strings.LastIndexByte(resURL, '?')
	if questMarkIdx >= 0 {
		resURL = resURL[:questMarkIdx]
	}

	// Removing the last slash
	if resURL[len(resURL)-1] == '/' {
		resURL = resURL[:len(resURL)-1]
	}

	// Getting schema
	schema := schemaHTTP
	schemaIdx := strings.Index(resURL, schemaSeparator)
	hasSchema := false
	if schemaIdx >= 0 {
		schema = resURL[:schemaIdx]
		resURL = resURL[schemaIdx+len(schemaSeparator):]
		hasSchema = true
	}

	// Removing credentials
	credIdx := strings.Index(resURL, "@")
	if credIdx >= 0 {
		resURL = resURL[credIdx+1:]
	}

	// Getting port
	port := "80"
	hasPort := false

	portIdx := strings.LastIndexByte(resURL, ':')
	if portIdx >= 0 {
		port = resURL[portIdx+1:]
		resURL = resURL[:portIdx]
		hasPort = true
	}

	if !hasSchema {
		switch port {
		case "80":
			schema = schemaHTTP

		case "443":
			schema = schemaHTTPS
		}
	} else if !hasPort {
		switch schema {
		case schemaHTTP:
			port = "80"
		case schemaHTTPS:
			port = "443"
		}
	}

	if len(resURL) == 0 {
		return "", fmt.Errorf("resolving [%v] to empty address", srcURL)
	}

	return schema + schemaSeparator + resURL + ":" + port, nil
}

func getSubject(topic string) string {
	return topic + "-value"
}

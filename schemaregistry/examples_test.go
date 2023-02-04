package schemaregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func createRegisterSchema(t *testing.T) *SchemaRegistry {
	t.Helper()

	mockCtrl := gomock.NewController(t)
	mockLogger := NewMockLogger(mockCtrl)

	mockLogger.EXPECT().Debug(gomock.Any()).AnyTimes().Do(func(msg string) { fmt.Println("DEBUG:", msg) })
	mockLogger.EXPECT().Debugf(gomock.Any(), gomock.Any()).AnyTimes().
		Do(func(format string, args ...interface{}) { fmt.Println("DEBUG:", fmt.Sprintf(format, args...)) })

	mockLogger.EXPECT().Error(gomock.Any()).AnyTimes().Do(func(msg string) { fmt.Println("ERROR:", msg) })
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any()).AnyTimes().
		Do(func(format string, args ...interface{}) { fmt.Println("ERROR:", fmt.Sprintf(format, args...)) })

	mockLogger.EXPECT().Info(gomock.Any()).AnyTimes().Do(func(msg string) { fmt.Println("INFO:", msg) })
	mockLogger.EXPECT().Infof(gomock.Any(), gomock.Any()).AnyTimes().
		Do(func(format string, args ...interface{}) { fmt.Println("INFO:", fmt.Sprintf(format, args...)) })

	schemaRegistry, err := NewSchemaRegistry(
		Config{
			Host:               "http://localhost:8081",
			Timeout:            15 * time.Second,
			InsecureSkipVerify: true,
			ConnectionTimeout:  30 * time.Second,
		},
		mockLogger,
	)

	require.NoError(t, err)

	return schemaRegistry
}

func TestSchemaRegistry_RegisterSchema(t *testing.T) {

	schemaRegistry := createRegisterSchema(t)

	const topicName = "test-topic-with-schema-1"

	schema, err := os.ReadFile("./testdata/data-definition-1.json")

	ctx := context.Background()

	schemaID, err := schemaRegistry.RegisterSchema(ctx, topicName, schema)
	require.NoError(t, err)

	fmt.Println("schemaID:", schemaID)

}

func TestSchemaRegistry_GetSchemaByID(t *testing.T) {

	schemaRegistry := createRegisterSchema(t)

	ctx := context.Background()

	schema, err := schemaRegistry.GetSchemaByID(ctx, 2)
	require.NoError(t, err)

	prettySchema := bytes.Buffer{}
	err = json.Indent(&prettySchema, []byte(schema), " ", " ")
	require.NoError(t, err)

	fmt.Println(prettySchema.String())
	fmt.Println()
}

func TestSchemaRegistry_GetLatestSchema(t *testing.T) {

	schemaRegistry := createRegisterSchema(t)

	const topicName = "test-topic-with-schema-1"

	ctx := context.Background()

	schemaData, err := schemaRegistry.GetLatestSchema(ctx, topicName)
	require.NoError(t, err)

	fmt.Println(schemaData.ID, schemaData.Subject, schemaData.Version)

	prettySchema := bytes.Buffer{}
	err = json.Indent(&prettySchema, []byte(schemaData.Schema), " ", " ")
	require.NoError(t, err)

	fmt.Println(prettySchema.String())
	fmt.Println()
}

func TestSchemaRegistry_IsLatestSchemaCompatible(t *testing.T) {

	schemaRegistry := createRegisterSchema(t)

	const topicName = "test-topic-with-schema-1"

	// testSchema := `{"type": "string"}`
	testSchema := `{
	"type": "object",
	"properties": {
		"string_val": {
			"type": "string"
		},

		"int_val": {
			"type": "integer"
		},

		"array_val": {
		  "type": "array",
		  "items": {
			"type": "string"
		  },
			"type": ["array", "null"]
		}
	}
}

`

	ctx := context.Background()

	isCompatible, err := schemaRegistry.IsLatestSchemaCompatible(ctx, topicName, testSchema)
	require.NoError(t, err)

	fmt.Println(isCompatible)
	fmt.Println()
}

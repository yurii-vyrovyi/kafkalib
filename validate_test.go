package kafkalib

import (
	"encoding/json"
	"fmt"
	"testing"

	gojson "github.com/goccy/go-json"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/stretchr/testify/require"
)

// validation packages options
// https://github.com/santhosh-tekuri/jsonschema
// https://github.com/qri-io/jsonschema
// https://github.com/xeipuuv/gojsonschema

func Test_JsonValidation(t *testing.T) {

	jsonSchema, err := jsonschema.Compile("../../models/data-definition.json")
	require.NoError(t, err)

	// 	jsonBuf := `{
	//     "string_val": "string value",
	//     "int_val": 155,
	//     "array_val": ["one", "two", "three"]
	// }`

	jsonBuf := `{
    "string_val": "string value",
    "int_val": 155,
    "array_val": null
}`
	var v interface{}
	err = json.Unmarshal([]byte(jsonBuf), &v)
	require.NoError(t, err)

	err = jsonSchema.Validate(v)
	require.NoError(t, err)

}

func Test_JsonParsing(t *testing.T) {

	jsonBuf := []byte(`{
	    "string_val": "string value",
	    "int_val": 155,
	    "array_val": ["one", "two", "three"]
	}`)

	var m map[string]interface{}

	err := gojson.Unmarshal(jsonBuf, &m)
	require.NoError(t, err)

	fmt.Println(m)

}

var (
	smallJson = []byte(`{
	"strVal1": "string value",
	"strVal2": "string value",
	"strVal3": "string value",

	"intVal1": 1,
	"intVal2": 1,
	"intVal3": 1,

	"objVal1": {"strKey": "val", "intVal": 15},
	"objVal2": {"strKey": "val", "intVal": 15},
	"objVal3": {"strKey": "val", "intVal": 15}
}`)

	middleJson = []byte(`{
	"strVal1":  "string value",
	"strVal2":  "string value",
	"strVal3":  "string value",
	"strVal4":  "string value",
	"strVal5":  "string value",
	"strVal6":  "string value",
	"strVal7":  "string value",
	"strVal8":  "string value",
	"strVal9":  "string value",
	"strVal10": "string value",
	"strVal11": "string value",
	"strVal12": "string value",
	"strVal13": "string value",
	"strVal14": "string value",
	"strVal15": "string value",
	"strVal16": "string value",
	"strVal17": "string value",
	"strVal18": "string value",
	"strVal19": "string value",
	"strVal20": "string value",

	"intVal1":  1,
	"intVal2":  1,
	"intVal3":  1,
	"intVal4":  1,
	"intVal5":  1,
	"intVal6":  1,
	"intVal7":  1,
	"intVal8":  1,
	"intVal9":  1,
	"intVal10": 1,
	"intVal11": 1,
	"intVal12": 1,
	"intVal13": 1,
	"intVal14": 1,
	"intVal15": 1,
	"intVal16": 1,
	"intVal17": 1,
	"intVal18": 1,
	"intVal19": 1,
	"intVal20": 1,

	"objVal1":  {"strKey": "val", "intVal": 15},
	"objVal2":  {"strKey": "val", "intVal": 15},
	"objVal3":  {"strKey": "val", "intVal": 15},
	"objVal4":  {"strKey": "val", "intVal": 15},
	"objVal5":  {"strKey": "val", "intVal": 15},
	"objVal6":  {"strKey": "val", "intVal": 15},
	"objVal7":  {"strKey": "val", "intVal": 15},
	"objVal8":  {"strKey": "val", "intVal": 15},
	"objVal9":  {"strKey": "val", "intVal": 15},
	"objVal10": {"strKey": "val", "intVal": 15},
	"objVal11": {"strKey": "val", "intVal": 15},
	"objVal12": {"strKey": "val", "intVal": 15},
	"objVal13": {"strKey": "val", "intVal": 15},
	"objVal14": {"strKey": "val", "intVal": 15},
	"objVal15": {"strKey": "val", "intVal": 15},
	"objVal16": {"strKey": "val", "intVal": 15},
	"objVal17": {"strKey": "val", "intVal": 15},
	"objVal18": {"strKey": "val", "intVal": 15},
	"objVal19": {"strKey": "val", "intVal": 15},
	"objVal20": {"strKey": "val", "intVal": 15}
}`)

	bigJson = []byte(`{
	"strVal1":  "string value",
	"strVal2":  "string value",
	"strVal3":  "string value",
	"strVal4":  "string value",
	"strVal5":  "string value",
	"strVal6":  "string value",
	"strVal7":  "string value",
	"strVal8":  "string value",
	"strVal9":  "string value",
	"strVal10": "string value",
	"strVal11": "string value",
	"strVal12": "string value",
	"strVal13": "string value",
	"strVal14": "string value",
	"strVal15": "string value",
	"strVal16": "string value",
	"strVal17": "string value",
	"strVal18": "string value",
	"strVal19": "string value",
	"strVal20": "string value",
	"strVal21": "string value",
	"strVal22": "string value",
	"strVal23": "string value",
	"strVal24": "string value",
	"strVal25": "string value",
	"strVal26": "string value",
	"strVal27": "string value",
	"strVal28": "string value",
	"strVal29": "string value",
	"strVal30": "string value",
	"strVal31": "string value",
	"strVal32": "string value",
	"strVal33": "string value",
	"strVal34": "string value",
	"strVal35": "string value",
	"strVal36": "string value",
	"strVal37": "string value",
	"strVal38": "string value",
	"strVal39": "string value",
	"strVal40": "string value",
	"strVal41": "string value",
	"strVal42": "string value",
	"strVal43": "string value",
	"strVal44": "string value",
	"strVal45": "string value",
	"strVal46": "string value",
	"strVal47": "string value",
	"strVal48": "string value",
	"strVal49": "string value",
	"strVal50": "string value",
	"strVal51": "string value",
	"strVal52": "string value",
	"strVal53": "string value",
	"strVal54": "string value",
	"strVal55": "string value",
	"strVal56": "string value",
	"strVal57": "string value",
	"strVal58": "string value",
	"strVal59": "string value",
	"strVal60": "string value",
	"strVal61": "string value",
	"strVal62": "string value",
	"strVal63": "string value",
	"strVal64": "string value",
	"strVal65": "string value",
	"strVal66": "string value",
	"strVal67": "string value",
	"strVal68": "string value",
	"strVal69": "string value",


	"intVal1":  1,
	"intVal2":  1,
	"intVal3":  1,
	"intVal4":  1,
	"intVal5":  1,
	"intVal6":  1,
	"intVal7":  1,
	"intVal8":  1,
	"intVal9":  1,
	"intVal10": 1,
	"intVal11": 1,
	"intVal12": 1,
	"intVal13": 1,
	"intVal14": 1,
	"intVal15": 1,
	"intVal16": 1,
	"intVal17": 1,
	"intVal18": 1,
	"intVal19": 1,
	"intVal20": 1,
	"intVal21": 1,
	"intVal22": 1,
	"intVal23": 1,
	"intVal24": 1,
	"intVal25": 1,
	"intVal26": 1,
	"intVal27": 1,
	"intVal28": 1,
	"intVal29": 1,
	"intVal30": 1,
	"intVal31": 1,
	"intVal32": 1,
	"intVal33": 1,
	"intVal34": 1,
	"intVal35": 1,
	"intVal36": 1,
	"intVal37": 1,
	"intVal38": 1,
	"intVal39": 1,
	"intVal40": 1,
	"intVal41": 1,
	"intVal42": 1,
	"intVal43": 1,
	"intVal44": 1,
	"intVal45": 1,
	"intVal46": 1,
	"intVal47": 1,
	"intVal48": 1,
	"intVal49": 1,
	"intVal50": 1,
	"intVal51": 1,
	"intVal52": 1,
	"intVal53": 1,
	"intVal54": 1,
	"intVal55": 1,
	"intVal56": 1,
	"intVal57": 1,
	"intVal58": 1,
	"intVal59": 1,
	"intVal60": 1,
	"intVal61": 1,
	"intVal62": 1,
	"intVal63": 1,
	"intVal64": 1,
	"intVal65": 1,
	"intVal66": 1,
	"intVal67": 1,
	"intVal68": 1,
	"intVal69": 1,


	"objVal1":  {"strKey": "val", "intVal": 15},
	"objVal2":  {"strKey": "val", "intVal": 15},
	"objVal3":  {"strKey": "val", "intVal": 15},
	"objVal4":  {"strKey": "val", "intVal": 15},
	"objVal5":  {"strKey": "val", "intVal": 15},
	"objVal6":  {"strKey": "val", "intVal": 15},
	"objVal7":  {"strKey": "val", "intVal": 15},
	"objVal8":  {"strKey": "val", "intVal": 15},
	"objVal9":  {"strKey": "val", "intVal": 15},
	"objVal10": {"strKey": "val", "intVal": 15},
	"objVal11": {"strKey": "val", "intVal": 15},
	"objVal12": {"strKey": "val", "intVal": 15},
	"objVal13": {"strKey": "val", "intVal": 15},
	"objVal14": {"strKey": "val", "intVal": 15},
	"objVal15": {"strKey": "val", "intVal": 15},
	"objVal16": {"strKey": "val", "intVal": 15},
	"objVal17": {"strKey": "val", "intVal": 15},
	"objVal18": {"strKey": "val", "intVal": 15},
	"objVal19": {"strKey": "val", "intVal": 15},
	"objVal20": {"strKey": "val", "intVal": 15},
	"objVal21": {"strKey": "val", "intVal": 15},
	"objVal22": {"strKey": "val", "intVal": 15},
	"objVal23": {"strKey": "val", "intVal": 15},
	"objVal24": {"strKey": "val", "intVal": 15},
	"objVal25": {"strKey": "val", "intVal": 15},
	"objVal26": {"strKey": "val", "intVal": 15},
	"objVal27": {"strKey": "val", "intVal": 15},
	"objVal28": {"strKey": "val", "intVal": 15},
	"objVal29": {"strKey": "val", "intVal": 15},
	"objVal30": {"strKey": "val", "intVal": 15},
	"objVal31": {"strKey": "val", "intVal": 15},
	"objVal32": {"strKey": "val", "intVal": 15},
	"objVal33": {"strKey": "val", "intVal": 15},
	"objVal34": {"strKey": "val", "intVal": 15},
	"objVal35": {"strKey": "val", "intVal": 15},
	"objVal36": {"strKey": "val", "intVal": 15},
	"objVal37": {"strKey": "val", "intVal": 15},
	"objVal38": {"strKey": "val", "intVal": 15},
	"objVal39": {"strKey": "val", "intVal": 15},
	"objVal40": {"strKey": "val", "intVal": 15},
	"objVal41": {"strKey": "val", "intVal": 15},
	"objVal42": {"strKey": "val", "intVal": 15},
	"objVal43": {"strKey": "val", "intVal": 15},
	"objVal44": {"strKey": "val", "intVal": 15},
	"objVal45": {"strKey": "val", "intVal": 15},
	"objVal46": {"strKey": "val", "intVal": 15},
	"objVal47": {"strKey": "val", "intVal": 15},
	"objVal48": {"strKey": "val", "intVal": 15},
	"objVal49": {"strKey": "val", "intVal": 15},
	"objVal50": {"strKey": "val", "intVal": 15},
	"objVal51": {"strKey": "val", "intVal": 15},
	"objVal52": {"strKey": "val", "intVal": 15},
	"objVal53": {"strKey": "val", "intVal": 15},
	"objVal54": {"strKey": "val", "intVal": 15},
	"objVal55": {"strKey": "val", "intVal": 15},
	"objVal56": {"strKey": "val", "intVal": 15},
	"objVal57": {"strKey": "val", "intVal": 15},
	"objVal58": {"strKey": "val", "intVal": 15},
	"objVal59": {"strKey": "val", "intVal": 15},
	"objVal60": {"strKey": "val", "intVal": 15},
	"objVal61": {"strKey": "val", "intVal": 15},
	"objVal62": {"strKey": "val", "intVal": 15},
	"objVal63": {"strKey": "val", "intVal": 15},
	"objVal64": {"strKey": "val", "intVal": 15},
	"objVal65": {"strKey": "val", "intVal": 15},
	"objVal66": {"strKey": "val", "intVal": 15},
	"objVal67": {"strKey": "val", "intVal": 15},
	"objVal68": {"strKey": "val", "intVal": 15},
	"objVal69": {"strKey": "val", "intVal": 15}
}`)
)

func benchmarkNativeJson(b *testing.B, jsonBuf []byte) {
	for n := 0; n < b.N; n++ {
		var m map[string]interface{}

		err := json.Unmarshal(jsonBuf, &m)
		require.NoError(b, err)
	}
}

func benchmarkGoJson(b *testing.B, jsonBuf []byte) {
	for n := 0; n < b.N; n++ {
		var m map[string]interface{}

		err := gojson.Unmarshal(jsonBuf, &m)
		require.NoError(b, err)
	}
}

func Benchmark_NativeJsonSmall(b *testing.B)  { benchmarkNativeJson(b, smallJson) }
func Benchmark_NativeJsonMiddle(b *testing.B) { benchmarkNativeJson(b, middleJson) }
func Benchmark_NativeJsonBig(b *testing.B)    { benchmarkNativeJson(b, bigJson) }

func Benchmark_GoJsonSmall(b *testing.B)  { benchmarkGoJson(b, smallJson) }
func Benchmark_GoJsonMiddle(b *testing.B) { benchmarkGoJson(b, middleJson) }
func Benchmark_GoJsonBig(b *testing.B)    { benchmarkGoJson(b, bigJson) }

package schemaregistry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_formatBaseURL(t *testing.T) {
	t.Parallel()

	type Test struct {
		srcURL string
		expURL string
		expErr bool
	}

	tests := map[string]Test{
		"OK": {
			srcURL: "http://schema.registry.host:8081",
			expURL: "http://schema.registry.host:8081",
			expErr: false,
		},

		"No schema, custom port": {
			srcURL: "schema.registry.host:8081",
			expURL: "http://schema.registry.host:8081",
			expErr: false,
		},

		"No schema, no port": {
			srcURL: "schema.registry.host",
			expURL: "http://schema.registry.host:80",
			expErr: false,
		},

		"No schema, 80 port": {
			srcURL: "schema.registry.host:80",
			expURL: "http://schema.registry.host:80",
			expErr: false,
		},

		"No schema, 443 port": {
			srcURL: "schema.registry.host:443",
			expURL: "https://schema.registry.host:443",
			expErr: false,
		},

		"http schema, 443 port": {
			srcURL: "http://schema.registry.host:443",
			expURL: "http://schema.registry.host:443",
			expErr: false,
		},

		"https schema, 80 port": {
			srcURL: "https://schema.registry.host:80",
			expURL: "https://schema.registry.host:80",
			expErr: false,
		},

		"with creds": {
			srcURL: "http://user:password@schema.registry.host:8081",
			expURL: "http://schema.registry.host:8081",
			expErr: false,
		},

		"with trailing slash": {
			srcURL: "http://schema.registry.host:8081/",
			expURL: "http://schema.registry.host:8081",
			expErr: false,
		},

		"with subdomain": {
			srcURL: "http://schema.registry.host:8081/some/more",
			expURL: "http://schema.registry.host:8081/some/more",
			expErr: false,
		},

		"with subdomain and params": {
			srcURL: "http://schema.registry.host:8081/some/more?param=value",
			expURL: "http://schema.registry.host:8081/some/more",
			expErr: false,
		},

		"empty": {
			srcURL: "",
			expURL: "",
			expErr: true,
		},

		"without host": {
			srcURL: "http://:8081",
			expURL: "",
			expErr: true,
		},
	}

	for description, test := range tests {
		test := test

		t.Run(description, func(t *testing.T) {
			t.Parallel()

			resURL, err := formatBaseURL(test.srcURL)

			if test.expErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			require.Equal(t, test.expURL, resURL)

		})
	}
}

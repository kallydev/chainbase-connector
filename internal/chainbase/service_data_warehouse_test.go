package chainbase_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/kallydev/chainbase-connector/internal/chainbase"
	"github.com/stretchr/testify/require"
)

func TestDataWarehouseService_Query(t *testing.T) {
	t.Parallel()

	client, err := chainbase.NewClient()
	require.NoError(t, err)

	type arguments struct {
		ctx   context.Context
		query string
	}

	testcases := []struct {
		name             string
		arguments        arguments
		wantResponse     require.ValueAssertionFunc
		wantHTTPResponse require.ValueAssertionFunc
		wantError        require.ErrorAssertionFunc
	}{
		{
			name: "Ping",
			arguments: arguments{
				ctx:   context.Background(),
				query: "SELECT 1;",
			},
			wantResponse: func(t require.TestingT, value interface{}, msgAndArgs ...interface{}) {
				response, ok := value.(*chainbase.Response[*chainbase.DataWarehouseData])
				require.True(t, ok, msgAndArgs...)

				require.Equal(t, 0, response.Code, msgAndArgs...)
			},
			wantHTTPResponse: func(t require.TestingT, value interface{}, msgAndArgs ...interface{}) {
				httpResponse, ok := value.(*http.Response)
				require.True(t, ok, msgAndArgs...)
				require.Equal(t, http.StatusOK, httpResponse.StatusCode, msgAndArgs...)
			},
			wantError: func(t require.TestingT, err error, msgAndArgs ...interface{}) {
				require.NoError(t, err, msgAndArgs...)
			},
		},
		{
			name: "Query the last 1,000 blocks in Ethereum",
			arguments: arguments{
				ctx:   context.Background(),
				query: "SELECT hash FROM ethereum.blocks ORDER BY number DESC LIMIT 1000;",
			},
			wantResponse: func(t require.TestingT, value interface{}, msgAndArgs ...interface{}) {
				response, ok := value.(*chainbase.Response[*chainbase.DataWarehouseData])
				require.True(t, ok, msgAndArgs...)

				require.Equal(t, 0, response.Code, msgAndArgs...)
			},
			wantHTTPResponse: func(t require.TestingT, value interface{}, msgAndArgs ...interface{}) {
				httpResponse, ok := value.(*http.Response)
				require.True(t, ok, msgAndArgs...)
				require.Equal(t, http.StatusOK, httpResponse.StatusCode, msgAndArgs...)
			},
			wantError: func(t require.TestingT, err error, msgAndArgs ...interface{}) {
				require.NoError(t, err, msgAndArgs...)
			},
		},
	}

	for _, testcase := range testcases {
		testcase := testcase

		t.Run(testcase.name, func(t *testing.T) {
			t.Parallel()

			response, httpResponse, err := client.DataWarehouse.Query(testcase.arguments.ctx, testcase.arguments.query)
			if testcase.wantError != nil {
				testcase.wantError(t, err)
			}

			if testcase.wantHTTPResponse != nil {
				testcase.wantHTTPResponse(t, httpResponse)
			}

			if testcase.wantResponse != nil {
				testcase.wantResponse(t, response)
			}
		})
	}
}

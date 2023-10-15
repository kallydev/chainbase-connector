package translator_test

import (
	"testing"

	"github.com/kallydev/chainbase-connector/internal/chainbase"
	"github.com/kallydev/chainbase-connector/internal/translator"
	"github.com/stretchr/testify/require"
)

func TestTranslate(t *testing.T) {
	type arguments struct {
		data chainbase.DataWarehouseData
	}

	testcases := []struct {
		name      string
		arguments arguments
		want      require.ValueAssertionFunc
		wantError require.ErrorAssertionFunc
	}{
		{
			name: "",
			arguments: arguments{
				data: chainbase.DataWarehouseData{
					TaskID:    "6d47920516f0451bbce84280d609de85",
					Rows:      5,
					RowsRead:  5647295,
					BytesRead: 468725485,
					Elapsed:   0.290984801,
					Meta: []chainbase.DataWarehouseDataMeta{
						{
							Name: "hash",
							Type: "String",
						},
					},
					Result: []map[string]any{
						{
							"hash": "0x629546f80a0c5bf3fdc1b607d6be630fb6307fbc0fd3198649c09b211512bd17",
						},
						{
							"hash": "0x7cfe52e27c5b9367a653d515e793ef4608e4ba7482cca9adc6161b017c4daa85",
						},
						{
							"hash": "0xd0cc6dffd80b907a1da7047ab5aef0ff4566292e510d67c2b9d0b343d0ba7a80",
						},
						{
							"hash": "0xfa03bd604f3eab81deacf950be0f8bd6a450b018ffb2c1632a0016d433e76337",
						},
						{
							"hash": "0xb4d3470cfa974212763710c110b77f4bed30905895b5afa5ea4b05e4166c2901",
						},
					},
					ErrMsg: "",
				},
			},
			want: func(t require.TestingT, value interface{}, msgAndArgs ...interface{}) {
				require.NotEmpty(t, value, msgAndArgs...)
			},
			wantError: func(t require.TestingT, err error, msgAndArgs ...interface{}) {
				require.NoError(t, err, msgAndArgs...)
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			data, err := translator.Translate(testcase.arguments.data)
			if testcase.wantError != nil {
				testcase.wantError(t, err)
			}

			if testcase.want != nil {
				testcase.want(t, data)
			}
		})
	}
}

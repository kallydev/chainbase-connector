package chainbase

import (
	"context"
	"fmt"
	"net/http"
)

type DataWarehouseService service

type DataWarehouseData struct {
	TaskID    string                  `json:"task_id"`
	Rows      int                     `json:"rows"`
	RowsRead  int                     `json:"rows_read"`
	BytesRead int                     `json:"bytes_read"`
	Elapsed   float32                 `json:"elapsed"`
	Meta      []DataWarehouseDataMeta `json:"meta"`
	Result    []map[string]any        `json:"result"`
	ErrMsg    string                  `json:"err_msg,omitempty"`
	NextPage  int                     `json:"next_page"`
}

type DataWarehouseDataMeta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (s *DataWarehouseService) Query(ctx context.Context, query string) (*Response[*DataWarehouseData], *http.Response, error) {
	params := map[string]any{
		"query": query,
	}

	httpRequest, err := s.client.NewRequest(ctx, http.MethodPost, "v1/dw/query", params)
	if err != nil {
		return nil, nil, fmt.Errorf("create request: %w", err)
	}

	var response Response[*DataWarehouseData]

	httpResponse, err := s.client.Do(ctx, httpRequest, &response)
	if err != nil {
		return nil, nil, fmt.Errorf("do request: %w", err)
	}

	return &response, httpResponse, nil
}

func (s *DataWarehouseService) Paginate(ctx context.Context, taskID string, page int) (*Response[*DataWarehouseData], *http.Response, error) {
	params := map[string]any{
		"task_id": taskID,
		"page":    page,
	}

	httpRequest, err := s.client.NewRequest(ctx, http.MethodPost, "v1/dw/query", params)
	if err != nil {
		return nil, nil, fmt.Errorf("create request: %w", err)
	}

	var response Response[*DataWarehouseData]

	httpResponse, err := s.client.Do(ctx, httpRequest, &response)
	if err != nil {
		return nil, nil, fmt.Errorf("do request: %w", err)
	}

	return &response, httpResponse, nil
}

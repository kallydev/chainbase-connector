package chainbase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/labstack/echo/v4"
	"github.com/samber/lo"
)

const (
	DefaultBaseURL   = "https://api.chainbase.online/"
	DefaultAPIKey    = "demo"
	DefaultUserAgent = "chainbase-connector"
)

const (
	CodeOK = 0
)

type Client struct {
	httpClient *http.Client
	service    service

	BaseURL   *url.URL
	UserAgent string
	APIKey    string

	DataWarehouse *DataWarehouseService
}

func (c *Client) NewRequest(ctx context.Context, method string, reference string, body any) (*http.Request, error) {
	requestURL, err := c.BaseURL.Parse(reference)
	if err != nil {
		return nil, fmt.Errorf("invalid reference: %w", err)
	}

	var buffer io.ReadWriter

	if body != nil {
		buffer = new(bytes.Buffer)
		if err := json.NewEncoder(buffer).Encode(body); err != nil {
			return nil, fmt.Errorf("encode body: %w", err)
		}
	}

	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), buffer)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	request.Header.Set("Accept", echo.MIMEApplicationJSONCharsetUTF8)

	if c.UserAgent != "" {
		request.Header.Set("User-Agent", c.UserAgent)
	}

	if c.APIKey != "" {
		request.Header.Set("X-API-Key", c.APIKey)
	}

	if body != nil {
		request.Header.Set("Content-Type", echo.MIMEApplicationJSONCharsetUTF8)
	}

	return request, nil
}

func (c *Client) Do(_ context.Context, request *http.Request, value any) (*http.Response, error) {
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}

	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return response, nil
}

func NewClient(options ...ClientOption) (*Client, error) {
	client := Client{
		httpClient: http.DefaultClient,
		UserAgent:  DefaultUserAgent,
		APIKey:     lo.Ternary(os.Getenv("CHAINBASE_API_KEY") == "", DefaultAPIKey, os.Getenv("CHAINBASE_API_KEY")),
	}

	baseURL, err := url.Parse(DefaultBaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base url: %w", err)
	}

	client.BaseURL = baseURL

	client.service.client = &client
	client.DataWarehouse = (*DataWarehouseService)(&client.service)

	for _, option := range options {
		if err := option(&client); err != nil {
			return nil, fmt.Errorf("apply option: %w", err)
		}
	}

	return &client, nil
}

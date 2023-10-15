package chainbase

type ClientOption func(*Client) error

func WithAPIKey(apiKey string) ClientOption {
	return func(client *Client) error {
		client.APIKey = apiKey

		return nil
	}
}

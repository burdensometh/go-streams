package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	ws "github.com/gorilla/websocket"
)

// HistoryFetcher retrieves historical records for the inclusive sequence
// range [from, to]. It is used by SequencedSource to backfill sequence gaps.
// Implementations must honor the context cancellation.
type HistoryFetcher interface {
	Fetch(ctx context.Context, from, to uint64) ([]Message, error)
}

// HistoryFetcherFunc adapts a function to the HistoryFetcher interface.
type HistoryFetcherFunc func(ctx context.Context, from, to uint64) ([]Message, error)

// Fetch implements HistoryFetcher.
func (f HistoryFetcherFunc) Fetch(ctx context.Context, from, to uint64) ([]Message, error) {
	return f(ctx, from, to)
}

// HTTPHistoryFetcher is a HistoryFetcher that queries an HTTP history
// endpoint for the missing sequence range.
type HTTPHistoryFetcher struct {
	endpoint   string
	client     *http.Client
	newRequest func(ctx context.Context, from, to uint64) (*http.Request, error)
	decode     func(body io.Reader) ([]Message, error)
}

var _ HistoryFetcher = (*HTTPHistoryFetcher)(nil)

// HTTPHistoryFetcherOption configures an HTTPHistoryFetcher.
type HTTPHistoryFetcherOption func(*HTTPHistoryFetcher)

// WithHTTPClient configures the HTTP client used for history requests.
// If not specified, http.DefaultClient is used.
func WithHTTPClient(client *http.Client) HTTPHistoryFetcherOption {
	return func(f *HTTPHistoryFetcher) {
		f.client = client
	}
}

// WithRequestBuilder configures a custom history request builder.
// If not specified, a GET request to the endpoint with "from" and "to"
// query parameters is issued.
func WithRequestBuilder(newRequest func(ctx context.Context, from, to uint64) (*http.Request,
	error)) HTTPHistoryFetcherOption {
	return func(f *HTTPHistoryFetcher) {
		f.newRequest = newRequest
	}
}

// WithResponseDecoder configures a custom history response decoder.
// If not specified, the response body is decoded as a JSON array of raw
// message payloads.
func WithResponseDecoder(decode func(body io.Reader) ([]Message, error)) HTTPHistoryFetcherOption {
	return func(f *HTTPHistoryFetcher) {
		f.decode = decode
	}
}

// NewHTTPHistoryFetcher returns a new HTTPHistoryFetcher for the endpoint.
func NewHTTPHistoryFetcher(endpoint string, opts ...HTTPHistoryFetcherOption) *HTTPHistoryFetcher {
	fetcher := &HTTPHistoryFetcher{
		endpoint:   endpoint,
		client:     http.DefaultClient,
		newRequest: defaultRequestBuilder(endpoint),
		decode:     decodeJSONMessages,
	}
	for _, opt := range opts {
		opt(fetcher)
	}
	return fetcher
}

// Fetch implements HistoryFetcher.
func (f *HTTPHistoryFetcher) Fetch(ctx context.Context, from, to uint64) ([]Message, error) {
	request, err := f.newRequest(ctx, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to build history request: %w", err)
	}
	response, err := f.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("history request failed: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode < http.StatusOK || response.StatusCode > 299 {
		return nil, fmt.Errorf("history request returned status %s", response.Status)
	}
	messages, err := f.decode(response.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to decode history response: %w", err)
	}
	return messages, nil
}

// defaultRequestBuilder returns a request builder that issues GET requests
// with "from" and "to" query parameters.
func defaultRequestBuilder(endpoint string) func(ctx context.Context, from, to uint64) (
	*http.Request, error) {
	return func(ctx context.Context, from, to uint64) (*http.Request, error) {
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, err
		}
		query := u.Query()
		query.Set("from", strconv.FormatUint(from, 10))
		query.Set("to", strconv.FormatUint(to, 10))
		u.RawQuery = query.Encode()
		return http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	}
}

// decodeJSONMessages decodes the response body as a JSON array of raw
// message payloads.
func decodeJSONMessages(body io.Reader) ([]Message, error) {
	var payloads []json.RawMessage
	if err := json.NewDecoder(body).Decode(&payloads); err != nil {
		return nil, err
	}
	messages := make([]Message, len(payloads))
	for i, payload := range payloads {
		messages[i] = Message{MsgType: ws.TextMessage, Payload: payload}
	}
	return messages, nil
}

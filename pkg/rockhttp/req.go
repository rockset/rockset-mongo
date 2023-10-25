package rockhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strings"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/rockset/rockset-go-client"
	"github.com/rockset/rockset-go-client/openapi"
)

type RocksetRawClient struct {
	c *rockset.RockClient
}

func NewRocksetRawClient(c *rockset.RockClient) *RocksetRawClient {
	return &RocksetRawClient{c}
}

func (c *RocksetRawClient) PreparePostRequest(ctx context.Context, path string, body map[string]interface{}) (*http.Request, error) {
	localBasePath, err := c.c.GetConfig().ServerURL(0, nil)
	if err != nil {
		return nil, &GenericOpenAPIError{error: err.Error()}
	}

	// Setup path and query parameters
	url, err := url.Parse(localBasePath + path)
	if err != nil {
		return nil, err
	}

	cfg := c.c.GetConfig()
	// Override request host, if applicable
	if cfg.Host != "" {
		url.Host = cfg.Host
	}

	// Override request scheme, if applicable
	if cfg.Scheme != "" {
		url.Scheme = cfg.Scheme
	}

	// Adding Query Param
	// Encode the parameters.
	var req *http.Request
	if body != nil {
		var bodyBytes []byte
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize request body: %w", err)
		}

		req, err = http.NewRequestWithContext(ctx, http.MethodPost, url.String(), bytes.NewBuffer(bodyBytes))
		req.Header.Add("Content-Type", "application/json")
	} else {
		req, err = http.NewRequestWithContext(ctx, http.MethodPost, url.String(), nil)
	}
	if err != nil {
		return nil, err
	}

	for k, v := range cfg.DefaultHeader {
		req.Header.Add(k, v)
	}
	req.Header.Add("User-Agent", cfg.UserAgent)

	return req, nil
}

func (c *RocksetRawClient) doCall(req *http.Request) (*http.Response, error) {
	cfg := c.c.GetConfig()
	if cfg.Debug {
		dump, err := httputil.DumpRequestOut(req, true)
		if err != nil {
			return nil, err
		}
		log.Logvf(log.Always, "\n%s\n", string(dump))
	}

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return resp, err
	}

	if cfg.Debug {
		dump, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return resp, err
		}
		log.Logvf(log.Always, "\n%s\n", string(dump))
	}
	return resp, err
}

func (c *RocksetRawClient) Execute(req *http.Request, respValue interface{}) (*http.Response, error) {

	resp, err := c.doCall(req)
	if err != nil || resp == nil {
		return resp, err
	}

	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewBuffer(respBody))
	if err != nil {
		return resp, err
	}

	if resp.StatusCode >= 300 {
		newErr := &GenericOpenAPIError{
			body:  respBody,
			error: resp.Status,
		}
		if len(respBody) > 0 && respBody[0] == '{' {
			var v openapi.ErrorModel
			err = json.Unmarshal(respBody, &v)
			if err != nil {
				newErr.error = err.Error()
				return resp, newErr
			}
			newErr.error = formatErrorMessage(resp.Status, &v)
			newErr.model = v
			log.Logvf(log.Always, "request failed: %+v", newErr.error)
		}
		return resp, newErr
	}

	err = json.Unmarshal(respBody, respValue)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return resp, nil
}

// GenericOpenAPIError Provides access to the body, error and model on returned errors.
type GenericOpenAPIError struct {
	body  []byte
	error string
	model interface{}
}

// Error returns non-empty string if there was an error.
func (e GenericOpenAPIError) Error() string {
	return e.error
}

// Body returns the raw bytes of the response
func (e GenericOpenAPIError) Body() []byte {
	return e.body
}

// Model returns the unpacked model of the error
func (e GenericOpenAPIError) Model() interface{} {
	return e.model
}

func formatErrorMessage(status string, v interface{}) string {
	str := ""
	metaValue := reflect.ValueOf(v).Elem()

	if metaValue.Kind() == reflect.Struct {
		field := metaValue.FieldByName("Title")
		if field != (reflect.Value{}) {
			str = fmt.Sprintf("%s", field.Interface())
		}

		field = metaValue.FieldByName("Detail")
		if field != (reflect.Value{}) {
			str = fmt.Sprintf("%s (%s)", str, field.Interface())
		}
	}

	return strings.TrimSpace(fmt.Sprintf("%s %s", status, str))
}

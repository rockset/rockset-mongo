package rockcollection

import (
	"context"
	"fmt"
	"strings"

	"github.com/rockset/rockset-go-client"
	"github.com/rockset/rockset-go-client/openapi"
	"github.com/rockset/rockset-mongo/pkg/config"
	"github.com/rockset/rockset-mongo/pkg/rockhttp"
)

type CollectionCreator struct {
	c   *rockset.RockClient
	raw *rockhttp.RocksetRawClient

	conf  *config.Config
	state *config.State
}

type CollectionState int

const (
	DOESNOT_EXIST CollectionState = iota
	INITIAL_LOAD_IN_PROGRESS
	INITIAL_LOAD_DONE
	STREAMING
)

func NewClient(conf *config.Config, state *config.State) (*CollectionCreator, error) {
	rc, err := rockset.NewClient(func(rc *rockset.RockConfig) {
		// environment variables overide config
		if rc.APIKey == "" {
			rc.APIKey = conf.Rockset.ApiKey
		}
		if rc.APIServer == "" {
			rc.APIKey = conf.Rockset.ApiServer
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create a Rockset client: %w", err)
	}

	c := &CollectionCreator{
		c:     rc,
		raw:   rockhttp.NewRocksetRawClient(rc),
		conf:  conf,
		state: state,
	}

	return c, nil
}

func (rc *CollectionCreator) CollectionState(coll *openapi.Collection) (CollectionState, error) {
	var s3, mongo *openapi.Source
	for _, s := range coll.Sources {
		if s.S3 != nil {
			s3 = &s
		} else if s.Mongodb != nil {
			mongo = &s
		}
	}

	if mongo != nil {
		return STREAMING, nil
	}

	if s3 == nil {
		return DOESNOT_EXIST, fmt.Errorf("unexpected state; no S3 or Mongo sources: %+v", coll.Sources)
	} else if state := sourceState(s3); state == "COMPLETED" || state == "WATCHING" {
		return INITIAL_LOAD_DONE, nil
	}
	return INITIAL_LOAD_IN_PROGRESS, nil
}

func sourceState(s *openapi.Source) string {
	if s == nil || s.Status == nil || s.Status.State == nil {
		return ""
	}

	return *s.Status.State
}

func (rc *CollectionCreator) GetCollection(ctx context.Context) (openapi.Collection, error) {
	return rc.c.GetCollection(ctx, rc.conf.Workspace, rc.conf.CollectionName)
}

func (rc *CollectionCreator) CreateInitialCollection(ctx context.Context) (*openapi.Collection, error) {
	body := map[string]interface{}{}
	for k, v := range rc.conf.CreateCollectionRequest {
		body[k] = v
	}

	body["collection"] = rc.conf.CollectionName
	body["sources"] = []map[string]interface{}{{
		"integration_name": rc.conf.S3.Integration,
		"format_params":    map[string]interface{}{"bson": true},
		"s3": map[string]interface{}{
			"bucket": rc.state.ExportInfo.Bucket,
			"prefix": rc.state.ExportInfo.Prefix,
		},
	}}

	path := fmt.Sprintf("/v1/orgs/self/ws/%s/collections", rc.conf.Workspace)

	req, err := rc.raw.PreparePostRequest(ctx, path, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create a question: %w", err)
	}

	var resp openapi.Collection
	_, err = rc.raw.Execute(req, &resp)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return &resp, nil
}

func (rc *CollectionCreator) AddMongoSource(ctx context.Context) (*openapi.GetSourceResponse, error) {
	hasTransformation := hasTransformation(rc.conf.CreateCollectionRequest)
	body := map[string]interface{}{
		"integration_name": rc.conf.Mongo.Integration,
		"mongodb": map[string]interface{}{
			"database_name":          rc.conf.Mongo.DB,
			"collection_name":        rc.conf.Mongo.Collection,
			"retrieve_full_document": hasTransformation,
			"initial_resume_token":   rc.state.CollectionInfo.ResumeToken,
		},
	}

	path := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s/sources", rc.conf.Workspace, rc.conf.CollectionName)

	req, err := rc.raw.PreparePostRequest(ctx, path, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create a question: %w", err)
	}

	var resp openapi.GetSourceResponse
	_, err = rc.raw.Execute(req, &resp)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return &resp, nil
}

func hasTransformation(s map[string]interface{}) bool {
	v, ok := s["field_mapping_query"]
	if !ok {
		return false
	}

	switch iv := v.(type) {
	case map[string]string:
		sql, ok := iv["sql"]
		return ok && strings.TrimSpace(sql) != ""
	case map[string]interface{}:
		if sql, ok := iv["sql"]; ok {
			if sqlStr, ok := sql.(string); ok {
				return strings.TrimSpace(sqlStr) != ""
			}
		}
		return false
	default:
		panic(fmt.Errorf("unsupported field_mapping_query: %T", iv))
	}
}
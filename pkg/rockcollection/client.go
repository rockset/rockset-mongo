package rockcollection

import (
	"context"
	"fmt"
	"strings"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/rockset/rockset-go-client"
	"github.com/rockset/rockset-go-client/openapi"
	"github.com/rockset/rockset-mongo/pkg/config"
	"github.com/rockset/rockset-mongo/pkg/rockhttp"
)

type CollectionCreator struct {
	c   *rockset.RockClient
	raw *rockhttp.RocksetRawClient

	conf *config.Config

	rocksetWorkspace  string
	rocksetCollection string
}

type CollectionState int

const (
	DOESNOT_EXIST CollectionState = iota
	INITIAL_LOAD_IN_PROGRESS
	INITIAL_LOAD_DONE
	STREAMING_WITH_S3
	STREAMING
)

func (s CollectionState) String() string {
	switch s {
	case DOESNOT_EXIST:
		return "DOESNOT_EXIST"
	case INITIAL_LOAD_IN_PROGRESS:
		return "INITIAL_LOAD_IN_PROGRESS"
	case INITIAL_LOAD_DONE:
		return "INITIAL_LOAD_DONE"
	case STREAMING_WITH_S3:
		return "STREAMING_WITH_S3"
	case STREAMING:
		return "STREAMING"
	default:
		return fmt.Sprintf("%d", s)
	}
}

func NewClient(conf *config.Config) (*CollectionCreator, error) {
	rc, err := rockset.NewClient(func(rc *rockset.RockConfig) {
		// environment variables overide config
		if rc.APIKey == "" {
			rc.APIKey = conf.Rockset.ApiKey
		}
		if rc.APIServer == "" {
			rc.APIServer = conf.Rockset.ApiServer
		}
		log.Logvf(log.DebugHigh, "create rc client api_key=%v api_server=%v", rc.APIKey, rc.APIServer)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create a Rockset client: %w", err)
	}

	workspace, collection := workspaceCollection(conf.RocksetCollection)
	c := &CollectionCreator{
		c:                 rc,
		raw:               rockhttp.NewRocksetRawClient(rc),
		conf:              conf,
		rocksetWorkspace:  workspace,
		rocksetCollection: collection,
	}

	return c, nil
}

func workspaceCollection(s string) (string, string) {
	idx := strings.LastIndexByte(s, '.')
	if idx < 0 {
		return "commons", s
	}
	return s[:idx], s[idx+1:]
}

func (rc *CollectionCreator) CollectionState(coll *openapi.Collection) (CollectionState, error) {
	if coll == nil {
		return DOESNOT_EXIST, nil
	}

	var s3, mongo *openapi.Source
	for _, s := range coll.Sources {
		if s.S3 != nil {
			s3 = &s
		} else if s.Mongodb != nil {
			mongo = &s
		}
	}

	if mongo != nil {
		if s3 == nil {
			return STREAMING, nil
		}
		return STREAMING_WITH_S3, nil
	}
	if s3 != nil {
		state := sourceState(s3)
		if state == "COMPLETED" || state == "WATCHING" {
			return INITIAL_LOAD_DONE, nil
		}
		return INITIAL_LOAD_IN_PROGRESS, nil
	}
	if rc.conf.LoadOnly {
		return STREAMING, nil
	}

	return DOESNOT_EXIST, fmt.Errorf("unexpected state; no S3 or Mongo sources: %+v", coll.Sources)
}

func sourceState(s *openapi.Source) string {
	if s == nil || s.Status == nil || s.Status.State == nil {
		return ""
	}

	return *s.Status.State
}

func (rc *CollectionCreator) GetCollection(ctx context.Context) (openapi.Collection, error) {
	return rc.c.GetCollection(ctx, rc.rocksetWorkspace, rc.rocksetCollection)
}

func (rc *CollectionCreator) DeleteSource(ctx context.Context, id string) error {
	req := rc.c.SourcesApi.DeleteSource(ctx, rc.rocksetWorkspace, rc.rocksetCollection, id)
	_, _, err := rc.c.SourcesApi.DeleteSourceExecute(req)
	return err
}

func (rc *CollectionCreator) CreateInitialCollection(ctx context.Context, exportInfo *config.ExportInfo) (*openapi.Collection, error) {
	body := map[string]interface{}{}
	for k, v := range rc.conf.CreateCollectionRequest {
		body[k] = v
	}

	body["name"] = rc.rocksetCollection
	body["sources"] = []map[string]interface{}{{
		"integration_name": rc.conf.S3.Integration,
		"format_params":    map[string]interface{}{"bson": true},
		"s3": map[string]interface{}{
			"bucket": exportInfo.Bucket,
			"prefix": exportInfo.Prefix,
		},
	}}

	path := fmt.Sprintf("/v1/orgs/self/ws/%s/collections", rc.rocksetWorkspace)

	log.Logvf(log.DebugLow, "creating with body %+v", body)
	req, err := rc.raw.PreparePostRequest(ctx, path, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create a collection: %w", err)
	}

	var resp openapi.Collection
	_, err = rc.raw.Execute(req, &resp)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return &resp, nil
}

func (rc *CollectionCreator) AddMongoSource(ctx context.Context, resumeToken string) (*openapi.GetSourceResponse, error) {
	hasTransformation := hasTransformation(rc.conf.CreateCollectionRequest)
	body := map[string]interface{}{
		"integration_name": rc.conf.Mongo.Integration,
		"mongodb": map[string]interface{}{
			"database_name":          rc.conf.Mongo.DB,
			"collection_name":        rc.conf.Mongo.Collection,
			"retrieve_full_document": hasTransformation,
			"initial_resume_token":   resumeToken,
		},
	}

	path := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s/sources", rc.rocksetWorkspace, rc.rocksetCollection)

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

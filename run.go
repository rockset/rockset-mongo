package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/rockset/rockset-go-client/openapi"
	"github.com/rockset/rockset-mongo/pkg/config"
	"github.com/rockset/rockset-mongo/pkg/mongo"
	"github.com/rockset/rockset-mongo/pkg/rockcollection"
	"github.com/rockset/rockset-mongo/pkg/writers"
)

type Driver struct {
	config   *config.Config
	state    *config.State
	dumpOpts options.ToolOptions

	creator    *rockcollection.CollectionCreator
	collection *openapi.Collection
}

func (d *Driver) preflight(ctx context.Context) error {
	if d.config.CollectionName == "" {
		return fmt.Errorf("missing `collection`")
	}
	if d.config.Workspace == "" {
		return fmt.Errorf("missing `workspace`")
	}
	return nil
}

func (d *Driver) prepare(ctx context.Context) error {
	d.config.Mongo.TargetChunkSize = 250 * 1024 * 1024
	if d.dumpOpts.DB != "" {
		d.config.Mongo.DB = d.dumpOpts.DB
	}
	if d.dumpOpts.Collection != "" {
		d.config.Mongo.Collection = d.dumpOpts.Collection
	}

	creator, err := rockcollection.NewClient(d.config, d.state)
	if err != nil {
		return fmt.Errorf("failed to create rockset api client: %w", err)
	}
	d.creator = creator
	return nil
}

func (d *Driver) finishedExport() bool {
	export := d.state.ExportInfo
	return export != nil && !export.EndTime.IsZero()
}

func (d *Driver) export(ctx context.Context) error {
	export := d.state.ExportInfo
	if export != nil && export.EndTime.IsZero() && !export.StartTime.IsZero() {
		log.Logvf(log.Always, "found a partial export, regenerating a new one")
		d.state.ID = uuid.New()
	}

	d.state.ExportInfo = &config.ExportInfo{
		StartTime: time.Now(),
	}

	progressManager := progress.NewBarWriter(log.Writer(0), progressBarWaitTime, progressBarLength, false)
	progressManager.Start()
	defer progressManager.Stop()

	dump := mongo.MongoDump{
		ToolOptions:     &d.dumpOpts,
		ProgressManager: progressManager,
	}

	if err := dump.Init(); err != nil {
		return fmt.Errorf("failed to export data: %w", err)
	}

	s3Uri := strings.TrimRight(d.config.S3.Uri, "/") + "/" + d.state.ID.String()
	out, err := writers.NewWriter(ctx, &writers.WriterOptions{
		Out:             s3Uri,
		TargetChunkSize: d.config.Mongo.TargetChunkSize,
		FilePrefix:      d.config.Mongo.DB + "." + d.config.Mongo.Collection,
	})
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	defer out.Close()

	info, err := dump.CollectionInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get collection info: %w", err)
	}
	d.state.CollectionInfo = info

	log.Logvf(log.Always, "Started export")
	if err = dump.Dump(ctx, out); err != nil {
		return fmt.Errorf("failed to export data: %w", err)
	}

	d.state.ExportInfo.EndTime = time.Now()
	d.state.ExportInfo.Bucket, d.state.ExportInfo.Prefix =
		d.bucketAndPrefix(s3Uri)

	return nil
}

func (d *Driver) bucketAndPrefix(uri string) (string, string) {
	parsed, err := url.ParseRequestURI(uri)
	if err != nil {
		panic(fmt.Errorf("invalid S3 path %v: %w", uri, err))
	}
	if parsed.Scheme != "s3" {
		panic(fmt.Errorf("path is not s3: %s", uri))
	}

	return parsed.Host, strings.Trim(parsed.Path, "/")
}

func (d *Driver) createCollection(ctx context.Context) error {
	_, err := d.creator.CreateInitialCollection(ctx)
	return err
}

func (d *Driver) waitUntilInitialLoadDone(ctx context.Context) error {
	for ctx.Err() == nil {
		coll, err := d.creator.GetCollection(ctx)
		if err != nil && !strings.Contains(err.Error(), "does not exist in") {
			return fmt.Errorf("failed to get collection info: %w", err)
		}

		collState, _ := d.creator.CollectionState(&coll)
		log.Logvf(log.Always, "collection state: %v", collState)

		if collState >= rockcollection.INITIAL_LOAD_DONE {
			return nil
		}

		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
	return fmt.Errorf("timed out before collection is ready: %w", ctx.Err())
}

func (d *Driver) createMongoDbSource(ctx context.Context) error {
	_, err := d.creator.AddMongoSource(ctx)
	return err
}

func (d *Driver) deleteS3Source(ctx context.Context) error {
	coll, err := d.creator.GetCollection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get collection info: %w", err)
	}

	var sourceId string
	for _, s := range coll.Sources {
		if s.S3 != nil {
			sourceId = *s.Id
		}
	}

	if sourceId == "" {
		log.Logvf(log.Always, "S3 source was deleted already")
		return nil
	}

	log.Logvf(log.Always, "Deleting S3 source %v", sourceId)
	return d.creator.DeleteSource(ctx, sourceId)
}

func (d *Driver) persistState() {
	err := d.state.WriteToFile("state.json")
	if err != nil {
		panic(fmt.Errorf("failed to persist state: %w", err))
	}
}

func (d *Driver) run(ctx context.Context) error {
	if err := d.preflight(ctx); err != nil {
		return fmt.Errorf("failed preflight checks: %w", err)

	}
	if err := d.prepare(ctx); err != nil {
		return fmt.Errorf("failed prepare checks: %w", err)
	}

	log.Logvf(log.Always, "exporting MongoDB data")
	if d.finishedExport() {
		log.Logvf(log.Always, "export is already done")
	} else {
		if err := d.export(ctx); err != nil {
			return fmt.Errorf("failed to export data: %w", err)
		}
	}
	d.persistState()

	log.Logvf(log.Always, "creating collection %v.%v", d.config.Workspace, d.config.CollectionName)
	// if err := d.createCollection(ctx); err != nil {
	// 	return fmt.Errorf("failed to create collection: %w", err)
	// }
	d.persistState()

	coll, err := d.creator.GetCollection(ctx)
	if err != nil && !strings.Contains(err.Error(), "does not exist in") {
		return fmt.Errorf("failed to get collection info: %w", err)
	}

	collState, _ := d.creator.CollectionState(&coll)
	log.Logvf(log.Always, "collection state: %v", collState)
	if collState <= rockcollection.DOESNOT_EXIST {
		if err := d.createCollection(ctx); err != nil {
			return fmt.Errorf("failed to create collection: %w", err)
		}
		d.persistState()
	}
	if collState <= rockcollection.INITIAL_LOAD_IN_PROGRESS {
		if err := d.waitUntilInitialLoadDone(ctx); err != nil {
			return fmt.Errorf("failed to wait for collection to be ready: %w", err)
		}
		d.persistState()
	}
	if collState <= rockcollection.INITIAL_LOAD_DONE {
		if err := d.createMongoDbSource(ctx); err != nil {
			return fmt.Errorf("failed to wait for collection to be ready: %w", err)
		}
		if err := d.deleteS3Source(ctx); err != nil {
			return fmt.Errorf("failed to wait for collection to be ready: %w", err)
		}
		d.persistState()
	}

	return nil
}
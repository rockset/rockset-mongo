package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
	bprogress "github.com/charmbracelet/bubbles/progress"
	"github.com/dustin/go-humanize"
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

	creator      *rockcollection.CollectionCreator
	collection   *openapi.Collection
	exportWriter writers.OutputWriter

	tui      bool
	logLevel int
}

func (d *Driver) preflight(ctx context.Context) error {
	if d.config.RocksetCollection == "" {
		return fmt.Errorf("missing rockset `collection`")
	}

	// check permissions to S3
	if err := d.checkS3Access(ctx); err != nil {
		return err
	}

	if err := d.checkRocksetAccess(ctx); err != nil {
		return err
	}

	return nil
}

func (d *Driver) prepare(ctx context.Context) error {
	d.config.Mongo.TargetChunkSizeMB = 250
	if d.dumpOpts.DB != "" {
		d.config.Mongo.DB = d.dumpOpts.DB
	} else {
		d.dumpOpts.DB = d.config.Mongo.DB
	}
	if d.dumpOpts.Collection != "" {
		d.config.Mongo.Collection = d.dumpOpts.Collection
	} else {
		d.dumpOpts.Collection = d.config.Mongo.Collection
	}

	creator, err := rockcollection.NewClient(d.config)
	if err != nil {
		return fmt.Errorf("failed to create rockset api client: %w", err)
	}
	d.creator = creator
	return nil
}

func (d *Driver) checkS3Access(ctx context.Context) error {
	cfg, err := awsConfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	uri, err := url.ParseRequestURI(d.config.S3.Uri)
	if err != nil {
		return fmt.Errorf("invalid S3 path %v: %w", d.config.S3.Uri, err)
	}
	if uri.Scheme != "s3" {
		return fmt.Errorf("path is not s3: %s", d.config.S3.Uri)
	}

	key := strings.Trim(uri.Path, "/") + "/" + uuid.NewString()
	s3Client := s3.NewFromConfig(cfg)
	_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(uri.Host),
		Key:    aws.String(key),
	})
	var errNotFound *types.NotFound
	if err == nil {
		return fmt.Errorf("found key unexpectedly; ensure that S3 path is empty: key=%v", key)
	} else if !errors.As(err, &errNotFound) {
		return fmt.Errorf("failed to query S3:  %w", err)
	}
	return nil
}

func (d *Driver) checkRocksetAccess(ctx context.Context) error {
	_, err := d.creator.GetCollection(ctx)
	if err != nil && !strings.Contains(err.Error(), "does not exist in") {
		return fmt.Errorf("failed to query Rockset: %w", err)
	}

	return nil
}

func (d *Driver) finishedExport() bool {
	export := d.state.ExportInfo
	return export != nil && !export.EndTime.IsZero()
}

func (d *Driver) progressor(totalDocuments uint64, dbNamespace string) (progress.Updateable, func()) {
	progressManager := progress.NewBarWriter(log.Writer(0), progressBarWaitTime, progressBarLength, false)
	progressManager.Start()

	dumpProgressor := progress.NewCounter(int64(totalDocuments))
	progressManager.Attach(dbNamespace, dumpProgressor)

	return dumpProgressor, func() {
		progressManager.Detach(dbNamespace)
		progressManager.Stop()
	}
}

func (d *Driver) export(ctx context.Context) error {
	var err error
	export := d.state.ExportInfo
	if export != nil && export.EndTime.IsZero() && !export.StartTime.IsZero() {
		log.Logvf(d.logLevel, "found a partial export, regenerating a new one")
		d.state.ID = uuid.New()
	}

	d.state.ExportInfo = &config.ExportInfo{
		StartTime: time.Now(),
	}

	dump := mongo.MongoDump{
		ToolOptions: &d.dumpOpts,
	}

	if err := dump.Init(); err != nil {
		return fmt.Errorf("failed to export data: %w", err)
	}

	s3Uri := strings.TrimRight(d.config.S3.Uri, "/") + "/" + d.state.ID.String()
	d.exportWriter, err = writers.NewWriter(ctx, &writers.WriterOptions{
		Out:             s3Uri,
		TargetChunkSize: d.config.Mongo.TargetChunkSizeMB * 1024 * 1024,
		FilePrefix:      d.config.Mongo.DB + "." + d.config.Mongo.Collection,
	})
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	defer d.exportWriter.Close()

	info, err := dump.CollectionInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get collection info: %w", err)
	}
	d.state.MongoDBCollectionInfo = info

	dbNamespace := d.dumpOpts.DB + "." + d.dumpOpts.Collection
	progressor, progressCleanup := d.progressor(info.Documents, dbNamespace)
	defer progressCleanup()

	log.Logvf(d.logLevel, "Started export to %v", s3Uri)
	if err = dump.Dump(ctx, d.exportWriter, progressor); err != nil {
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
	_, err := d.creator.CreateInitialCollection(ctx, d.state.ExportInfo)
	return err
}

func (d *Driver) waitUntilInitialLoadDone(ctx context.Context) error {
	for ctx.Err() == nil {
		coll, err := d.creator.GetCollection(ctx)
		if err != nil && !strings.Contains(err.Error(), "does not exist in") {
			return fmt.Errorf("failed to get collection info: %w", err)
		}

		collState, _ := d.creator.CollectionState(&coll)
		log.Logvf(d.logLevel, "collection state: %v", collState)

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

func (d *Driver) waitUntilReady(ctx context.Context) error {
	for ctx.Err() == nil {
		coll, err := d.creator.GetCollection(ctx)
		if err != nil && !strings.Contains(err.Error(), "does not exist in") {
			return fmt.Errorf("failed to get collection info: %w", err)
		}
		status := ""
		if coll.Status != nil {
			status = strings.ToUpper(*coll.Status)

		}
		log.Logvf(d.logLevel, "collection state: %v", status)

		if status == "READY" {
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
	_, err := d.creator.AddMongoSource(ctx, d.state.MongoDBCollectionInfo.ResumeToken)
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
		log.Logvf(d.logLevel, "S3 source was deleted already")
		return nil
	}

	log.Logvf(d.logLevel, "Deleting S3 source %v", sourceId)
	return d.creator.DeleteSource(ctx, sourceId)
}

func (d *Driver) persistState() {
	err := d.state.WriteToFile("state.json")
	if err != nil {
		panic(fmt.Errorf("failed to persist state: %w", err))
	}
}

func (d *Driver) getCollection(ctx context.Context) (*openapi.Collection, error) {
	coll, err := d.creator.GetCollection(ctx)
	if err != nil {
		if !strings.Contains(err.Error(), "does not exist in") {
			return nil, fmt.Errorf("failed to get collection info: %w", err)
		}
		d.collection = nil
		return nil, nil
	}

	d.collection = &coll
	return &coll, err

}

func (d *Driver) run(ctx context.Context) error {
	if err := d.prepare(ctx); err != nil {
		return fmt.Errorf("failed prepare checks: %w", err)
	}

	if err := d.preflight(ctx); err != nil {
		return fmt.Errorf("failed preflight checks: %w", err)
	}

	if d.finishedExport() {
		log.Logvf(d.logLevel, "export is already done")
	} else {
		if err := d.export(ctx); err != nil {
			return fmt.Errorf("failed to export data: %w", err)
		}
	}
	d.persistState()

	log.Logvf(d.logLevel, "creating collection %v", d.config.RocksetCollection)

	coll, err := d.getCollection(ctx)
	if err != nil && !strings.Contains(err.Error(), "does not exist in") {
		return fmt.Errorf("failed to get collection info: %w", err)
	}

	collState, _ := d.creator.CollectionState(coll)
	log.Logvf(d.logLevel, "collection state: %v", collState)
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
	}
	if collState <= rockcollection.STREAMING_WITH_S3 {
		if err := d.waitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed to wait for collection to be ready: %w", err)
		}
		if err := d.deleteS3Source(ctx); err != nil {
			return fmt.Errorf("failed to wait for collection to be ready: %w", err)
		}
		d.persistState()
	}

	return nil
}

func (d *Driver) stateString() string {
	var s strings.Builder

	s.WriteString("### Exporting MongoDB Collection\n")
	if d.state != nil && d.state.MongoDBCollectionInfo != nil {
		s.WriteString(fmt.Sprintf(" Docs: %v    Size: %v\n", humanize.Comma(int64(d.state.MongoDBCollectionInfo.Documents)),
			humanize.Bytes(d.state.MongoDBCollectionInfo.Size)))
	}
	if !d.finishedExport() && d.exportWriter != nil {
		stats := d.exportWriter.Stats()
		docs := atomic.LoadUint64(&stats.Docs)
		perc := float64(atomic.LoadUint64(&stats.Docs)) / float64(d.state.MongoDBCollectionInfo.Documents)
		if perc > 1 {
			perc = 1
		}
		prog := bprogress.New(bprogress.WithScaledGradient("#FF7CCB", "#FDFF8C"))
		s.WriteString(prog.ViewAs(perc))
		s.WriteString("  " + humanize.Comma(int64(docs)) + " / " + humanize.Comma(int64(d.state.MongoDBCollectionInfo.Documents)) + "\n")
	}

	if d.finishedExport() {
		s.WriteString("  Export done\n")
		s.WriteString("\n")
		s.WriteString("### Creating Rockset collection ")
		s.WriteString(d.config.RocksetCollection)
		s.WriteString("\n")

		coll := d.collection
		if coll == nil {
			s.WriteString(" Creating collection")
		} else {
			s.WriteString(" Status: " + *coll.Status)

			var s3 *openapi.Source
			for _, cs := range coll.Sources {
				if cs.S3 != nil {
					s3 = &cs
				}
			}

			if s3 != nil && s3.S3.ObjectCountDownloaded != nil && s3.S3.ObjectCountTotal != nil {
				downloaded := *s3.S3.ObjectCountDownloaded
				total := *s3.S3.ObjectCountTotal

				s.WriteString("Processing S3 objects\n")
				prog := bprogress.New(bprogress.WithScaledGradient("#FF7CCB", "#FDFF8C"))
				s.WriteString(prog.ViewAs(1.0 * float64(downloaded) / float64(total)))
				s.WriteString(fmt.Sprintf("  %v / %v\n",
					humanize.Comma(downloaded), humanize.Comma(total)))
			}
		}
	}

	s.WriteString("\n\n")
	return s.String()
}

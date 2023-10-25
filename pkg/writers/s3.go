package writers

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/mongodb/mongo-tools/common/log"
)

type S3Writer struct {
	ctx    context.Context
	bucket string
	prefix string

	fs       *DirectoryWriter
	uploadWg sync.WaitGroup

	uploadErrMtx sync.Mutex
	uploadErr    error

	s3Client *s3.Client
	uploader *manager.Uploader
}

var _ OutputWriter = (*S3Writer)(nil)

// Close implements io.WriteCloser.
func NewS3Writer(ctx context.Context, opts *WriterOptions) (*S3Writer, error) {
	uri, err := url.ParseRequestURI(opts.Out)
	if err != nil {
		return nil, fmt.Errorf("invalid S3 path %v: %w", opts.Out, err)
	}
	if uri.Scheme != "s3" {
		return nil, fmt.Errorf("path is not s3: %s", opts.Out)
	}
	log.Logvf(log.Always, "uri %+v", uri)

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	w := &S3Writer{
		ctx:    ctx,
		bucket: uri.Host,
		prefix: strings.Trim(uri.Path, "/"),

		fs:       &DirectoryWriter{},
		s3Client: s3Client,
		uploader: manager.NewUploader(s3Client),
	}
	w.fs, err = NewDirectoryWriter("s3buffer", opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create local dir: %w", err)
	}
	w.fs.onClosed = w.fileClosed
	return w, nil
}

func (w *S3Writer) Close() error {
	if err := w.fs.Close(); err != nil {
		return fmt.Errorf("failed to close fs: %w", err)
	}

	w.uploadWg.Wait()
	return w.uploadError()
}

func (w *S3Writer) Write(p []byte) (int, error) {
	return w.fs.Write(p)
}

func (w *S3Writer) uploadError() error {
	w.uploadErrMtx.Lock()
	defer w.uploadErrMtx.Unlock()
	return w.uploadErr
}

func (w *S3Writer) fileClosed(f *os.File) error {
	if err := w.uploadError(); err != nil {
		return err
	}

	w.uploadWg.Add(1)
	go func() {
		defer w.uploadWg.Done()

		err := w.uploadFile(f)
		log.Logvf(log.DebugHigh, "uploaded %s: %v", f.Name(), err)

		if err != nil {
			w.uploadErrMtx.Lock()
			w.uploadErr = err
			w.uploadErrMtx.Unlock()
		}
	}()
	return nil
}

func (w *S3Writer) uploadFile(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to beginning of file %v: %v", f.Name(), err)
	}

	_, err := w.uploader.Upload(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.prefix + "/" + filepath.Base(f.Name())),
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("failed to upload %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close to file %v: %w", f.Name(), err)
	}

	if err := os.Remove(f.Name()); err != nil {
		return fmt.Errorf("failed to rm to file %v: %w", f.Name(), err)
	}

	return nil
}

func (w *S3Writer) Stats() Stats {
	return w.fs.stats
}

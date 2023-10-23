package main

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

func NewWriter(ctx context.Context, opts *Options) (io.WriteCloser, error) {
	if opts.Out == "-" {
		return &nopCloserWriter{os.Stdout}, nil
	} else if strings.HasPrefix(opts.Out, "s3://") {
		return NewS3Writer(ctx, opts)
	}
	return NewDirectoryWriter(opts.Out, opts)
}

type nopCloserWriter struct {
	io.Writer
}

// Close implements io.WriteCloser.
func (*nopCloserWriter) Close() error {
	return nil
}

type DirectoryWriter struct {
	dirPath    string
	sizeCutoff uint64
	fileFormat string

	idx          uint32
	writtenBytes uint64
	f            *os.File

	onClosed func(f *os.File) error
}

var _ io.WriteCloser = (*DirectoryWriter)(nil)

func NewDirectoryWriter(out string, opts *Options) (*DirectoryWriter, error) {
	if _, err := os.Stat(out); err == nil {
		return nil, fmt.Errorf("path %s exists already", out)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("unexpected error: %w", err)
	}

	if err := os.MkdirAll(out, 0700); err != nil {
		return nil, fmt.Errorf("failed to create dir %s: %w", out, err)
	}

	return &DirectoryWriter{
		dirPath:    out,
		sizeCutoff: uint64(opts.OutputOptions.TargetSize) * 1024 * 1024,
		fileFormat: opts.Collection + ".%04d",
	}, nil

}

// Close implements io.WriteCloser.
func (w *DirectoryWriter) Close() error {
	if err := w.closeFile(w.f); err != nil {
		return err
	}
	return nil
}

func (w *DirectoryWriter) closeFile(f *os.File) error {
	if f == nil {
		return nil
	}
	if w.onClosed != nil {
		if err := w.onClosed(f); err != nil {
			return fmt.Errorf("file close back failed for %v: %w", f.Name(), err)
		}
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close a file %s: %s", f.Name(), err)
	}

	return nil
}

// Write implements io.WriteCloser.
func (w *DirectoryWriter) Write(p []byte) (int, error) {
	if err := w.maybeRotate(); err != nil {
		return 0, err
	}

	n, err := w.f.Write(p)
	w.writtenBytes += uint64(n)
	return n, err
}

func (w *DirectoryWriter) maybeRotate() error {
	if w.f != nil && (w.sizeCutoff == 0 || w.writtenBytes < w.sizeCutoff) {
		return nil
	}

	if err := w.closeFile(w.f); err != nil {
		return err
	}

	suffix := w.idx
	w.idx++

	var err error
	w.f, err = os.Create(filepath.Join(w.dirPath, fmt.Sprintf(w.fileFormat, suffix)))
	if err != nil {
		return fmt.Errorf("failed to create new file: %w", err)
	}
	w.writtenBytes = 0

	return nil
}

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

var _ io.WriteCloser = (*S3Writer)(nil)

// Close implements io.WriteCloser.
func NewS3Writer(ctx context.Context, opts *Options) (io.WriteCloser, error) {
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

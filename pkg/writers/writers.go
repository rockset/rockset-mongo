package writers

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type WriterOptions struct {
	Out             string
	TargetChunkSize uint64
	FilePrefix      string
}

type Stats struct {
	docs  uint64
	bytes uint64
	files uint64
}

type OutputWriter interface {
	// Write a single bson document
	Write(p []byte) (n int, err error)

	io.Closer

	Stats() Stats
}

func NewWriter(ctx context.Context, opts *WriterOptions) (OutputWriter, error) {
	if opts.Out == "-" {
		return NewStreamWrapper(os.Stdout), nil
	} else if strings.HasPrefix(opts.Out, "s3://") {
		return NewS3Writer(ctx, opts)
	}
	return NewDirectoryWriter(opts.Out, opts)
}

type streamWrapper struct {
	writer io.Writer
	stats  Stats
}

func NewStreamWrapper(writer io.Writer) *streamWrapper {
	return &streamWrapper{
		writer: writer,
		stats:  Stats{files: 1},
	}
}

// Close implements io.WriteCloser.
func (w *streamWrapper) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.stats.docs++
	w.stats.bytes += uint64(n)
	return n, err
}

func (w *streamWrapper) Close() error {
	return nil
}

func (w *streamWrapper) Stats() Stats {
	return w.stats
}

type DirectoryWriter struct {
	dirPath    string
	sizeCutoff uint64
	fileFormat string

	idx          uint32
	writtenBytes uint64
	f            *os.File

	onClosed func(f *os.File) error
	stats    Stats
}

var _ OutputWriter = (*DirectoryWriter)(nil)

func NewDirectoryWriter(out string, opts *WriterOptions) (*DirectoryWriter, error) {
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
		sizeCutoff: uint64(opts.TargetChunkSize),
		fileFormat: opts.FilePrefix + ".%04d",
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
// FIXME: This assumes that the bson document is written enitrely in a single call;
// Otherwise, a doc might be torn and shredded in multiple files
func (w *DirectoryWriter) Write(p []byte) (int, error) {
	if err := w.maybeRotate(); err != nil {
		return 0, err
	}

	n, err := w.f.Write(p)
	w.stats.docs++
	w.stats.bytes += uint64(n)
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
	w.stats.files++

	return nil
}

func (w *DirectoryWriter) Stats() Stats {
	return w.stats
}

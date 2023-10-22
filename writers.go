package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func NewWriter(opts *Options) (io.WriteCloser, error) {
	if opts.Out == "-" {
		return &nopCloserWriter{os.Stdout}, nil
	} else if strings.HasPrefix(opts.Out, "s3://") {
		return nil, fmt.Errorf("s3 not implemented yet")
	} else {
		if _, err := os.Stat(opts.Out); err == nil {
			return nil, fmt.Errorf("path %s exists already", opts.Out)
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("unexpected error: %w", err)
		}

		if err := os.MkdirAll(opts.Out, 0700); err != nil {
			return nil, fmt.Errorf("failed to create dir %s: %w", opts.Out, err)
		}

		return &DirectoryWriter{
			dirPath:    opts.Out,
			sizeCutoff: uint64(opts.OutputOptions.TargetSize) * 1024 * 1024,
			fileFormat: opts.Collection + ".%04d",
		}, nil
	}
	return nil, fmt.Errorf("unexpected format %s", opts.Out)
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
}

var _ io.WriteCloser = (*DirectoryWriter)(nil)

// Close implements io.WriteCloser.
func (w *DirectoryWriter) Close() error {
	if w.f != nil {
		return w.f.Close()
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

	if w.f != nil {
		if err := w.f.Close(); err != nil {
			return fmt.Errorf("failed to close a file %s: %s", w.f.Name(), err)
		}
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

package main

import (
	"fmt"
	"io"
	"os"
)

func NewWriter(output OutputOptions) (io.WriteCloser, error) {
	if output.Out == "-" {
		return &nopCloserWriter{os.Stdout}, nil
	}
	return nil, fmt.Errorf("unexpected format %s", output.Out)
}

type nopCloserWriter struct {
	io.Writer
}

// Close implements io.WriteCloser.
func (*nopCloserWriter) Close() error {
	return nil
}

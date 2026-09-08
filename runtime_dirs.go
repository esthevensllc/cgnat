package main

import (
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// ensureWritableDirectory creates a runtime directory and verifies the service
// account can create and remove files there before traffic is accepted.
func ensureWritableDirectory(path string, mode fs.FileMode) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("directory path is empty")
	}

	if err := os.MkdirAll(path, mode); err != nil {
		return fmt.Errorf("create directory %s: %w", path, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat directory %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", path)
	}

	probe, err := os.CreateTemp(path, ".huawei-cgn-write-check-*")
	if err != nil {
		return fmt.Errorf("write check directory %s: %w", path, err)
	}
	probePath := probe.Name()
	if err := probe.Close(); err != nil {
		_ = os.Remove(probePath)
		return fmt.Errorf("close write check directory %s: %w", path, err)
	}
	if err := os.Remove(probePath); err != nil {
		return fmt.Errorf("remove write check directory %s: %w", path, err)
	}

	return nil
}

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureWritableDirectoryCreatesNestedDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "failed", "reprocessed")
	if err := ensureWritableDirectory(directory, 0750); err != nil {
		t.Fatalf("ensureWritableDirectory() error = %v", err)
	}

	info, err := os.Stat(directory)
	if err != nil {
		t.Fatalf("stat directory: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("path %s is not a directory", directory)
	}
}

func TestEnsureWritableDirectoryRejectsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("test"), 0600); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	if err := ensureWritableDirectory(path, 0750); err == nil {
		t.Fatal("ensureWritableDirectory() error = nil, want error for file path")
	}
}

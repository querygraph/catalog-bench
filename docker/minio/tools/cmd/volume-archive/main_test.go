package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBackupRestoreRoundTrip(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	restored := filepath.Join(root, "restored")
	archive := filepath.Join(root, "backup.tar.gz")
	if err := os.MkdirAll(filepath.Join(source, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "nested", "state.db"), []byte("durable-state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(restored, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := backup(source, archive); err != nil {
		t.Fatal(err)
	}
	if err := restore(restored, archive); err != nil {
		t.Fatal(err)
	}
	value, err := os.ReadFile(filepath.Join(restored, "nested", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "durable-state" {
		t.Fatalf("restored value=%q", value)
	}
}

func TestRestoreRejectsNonEmptyDirectory(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	restored := filepath.Join(root, "restored")
	archive := filepath.Join(root, "backup.tar.gz")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "state"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := backup(source, archive); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(restored, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(restored, "existing"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := restore(restored, archive); err == nil {
		t.Fatal("restore should reject a non-empty target")
	}
}

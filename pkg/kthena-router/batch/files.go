/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package batch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ErrTooLarge means the upload passed the configured byte limit.
var ErrTooLarge = errors.New("batch: file is too large")

const (
	fileMode = 0o640
	dirMode  = 0o750
)

// idPattern is what a server-generated file or batch id may look like. Nothing that
// reaches the filesystem is ever taken from client input.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// FileStore holds the bytes of batch input and output files on a volume shared by
// every router replica.
type FileStore interface {
	// Create streams up to limit bytes into the store and replaces any earlier
	// content atomically. It returns how many bytes were written.
	Create(ctx context.Context, tenant, id string, r io.Reader, limit int64) (int64, error)
	// Open returns the file for reading, or ErrNotFound.
	Open(ctx context.Context, tenant, id string) (*os.File, error)
	// Size returns the byte count of a stored file, or ErrNotFound.
	Size(ctx context.Context, tenant, id string) (int64, error)
	// Remove deletes the bytes, or returns ErrNotFound.
	Remove(ctx context.Context, tenant, id string) error
	// Probe checks at startup that the volume is really writable by this pod.
	Probe(ctx context.Context) error
}

// FSStore is the shared-volume implementation of FileStore.
type FSStore struct {
	root string
}

var _ FileStore = (*FSStore)(nil)

// NewFSStore roots a file store at the mounted volume.
func NewFSStore(root string) *FSStore {
	return &FSStore{root: root}
}

// path keeps each tenant in its own directory, named by a hash so no tenant string
// ever reaches the filesystem.
func (s *FSStore) path(tenant, id string) (string, error) {
	if !idPattern.MatchString(id) {
		return "", fmt.Errorf("batch: invalid file id %q", id)
	}
	sum := sha256.Sum256([]byte(tenant))
	return filepath.Join(s.root, hex.EncodeToString(sum[:8]), id), nil
}

// Create streams the reader into the store and swaps it in with a rename.
func (s *FSStore) Create(ctx context.Context, tenant, id string, r io.Reader, limit int64) (int64, error) {
	final, err := s.path(tenant, id)
	if err != nil {
		return 0, err
	}
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return 0, err
	}
	temp, err := os.CreateTemp(dir, "."+id+".*")
	if err != nil {
		return 0, err
	}
	written, err := s.fill(ctx, temp, r, limit)
	if err != nil {
		temp.Close()
		os.Remove(temp.Name())
		return 0, err
	}
	if err := temp.Chmod(fileMode); err != nil {
		temp.Close()
		os.Remove(temp.Name())
		return 0, err
	}
	if err := temp.Close(); err != nil {
		os.Remove(temp.Name())
		return 0, err
	}
	if err := os.Rename(temp.Name(), final); err != nil {
		os.Remove(temp.Name())
		return 0, err
	}
	return written, syncDir(dir)
}

func (s *FSStore) fill(ctx context.Context, f *os.File, r io.Reader, limit int64) (int64, error) {
	if limit > 0 {
		r = io.LimitReader(r, limit+1)
	}
	written, err := io.Copy(f, r)
	if err != nil {
		return 0, err
	}
	if limit > 0 && written > limit {
		return 0, ErrTooLarge
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return written, f.Sync()
}

// Open returns the stored file for reading.
func (s *FSStore) Open(ctx context.Context, tenant, id string) (*os.File, error) {
	path, err := s.path(tenant, id)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}

// Size reports how many bytes are stored.
func (s *FSStore) Size(ctx context.Context, tenant, id string) (int64, error) {
	path, err := s.path(tenant, id)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Remove deletes the stored bytes.
func (s *FSStore) Remove(ctx context.Context, tenant, id string) error {
	path, err := s.path(tenant, id)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return err
	}
	return syncDir(filepath.Dir(path))
}

// Probe writes, syncs, renames and deletes a file, because a mounted volume can still
// be read-only for the user this pod runs as.
func (s *FSStore) Probe(ctx context.Context) error {
	dir := filepath.Join(s.root, ".probe")
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return fmt.Errorf("batch: storage %s is not writable: %w", s.root, err)
	}
	defer os.RemoveAll(dir)
	temp, err := os.CreateTemp(dir, "probe.*")
	if err != nil {
		return fmt.Errorf("batch: storage %s is not writable: %w", s.root, err)
	}
	name := temp.Name()
	if _, err := temp.WriteString("kthena"); err != nil {
		temp.Close()
		return fmt.Errorf("batch: storage %s is not writable: %w", s.root, err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("batch: storage %s cannot be synced: %w", s.root, err)
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, name+".moved"); err != nil {
		return fmt.Errorf("batch: storage %s does not support rename: %w", s.root, err)
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) && !strings.Contains(err.Error(), "invalid argument") {
		return err
	}
	return nil
}

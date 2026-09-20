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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestFileStore writes to a temporary directory, or under KTHENA_TEST_FILE_ROOT
// when that is set, so the same tests can run against a real mounted volume.
func newTestFileStore(t *testing.T) (*FSStore, string) {
	t.Helper()
	root := t.TempDir()
	if shared := os.Getenv("KTHENA_TEST_FILE_ROOT"); shared != "" {
		root = filepath.Join(shared, strings.ReplaceAll(t.Name(), "/", "_"))
		require.NoError(t, os.MkdirAll(root, dirMode))
		t.Cleanup(func() { _ = os.RemoveAll(root) })
	}
	return NewFSStore(root), root
}

func readAll(t *testing.T, store *FSStore, tenant, id string) string {
	t.Helper()
	f, err := store.Open(context.Background(), tenant, id)
	require.NoError(t, err)
	defer f.Close()
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	return string(data)
}

func TestFileStoreCreateAndRead(t *testing.T) {
	store, _ := newTestFileStore(t)
	ctx := context.Background()

	written, err := store.Create(ctx, "alice", "file-1", strings.NewReader("hello\nworld\n"), 1024)
	require.NoError(t, err)
	assert.Equal(t, int64(12), written)
	assert.Equal(t, "hello\nworld\n", readAll(t, store, "alice", "file-1"))

	size, err := store.Size(ctx, "alice", "file-1")
	require.NoError(t, err)
	assert.Equal(t, int64(12), size)
}

func TestFileStoreKeepsTenantsApart(t *testing.T) {
	store, _ := newTestFileStore(t)
	ctx := context.Background()

	_, err := store.Create(ctx, "alice", "file-1", strings.NewReader("alice data"), 1024)
	require.NoError(t, err)
	_, err = store.Create(ctx, "bob", "file-1", strings.NewReader("bob data"), 1024)
	require.NoError(t, err)

	assert.Equal(t, "alice data", readAll(t, store, "alice", "file-1"))
	assert.Equal(t, "bob data", readAll(t, store, "bob", "file-1"), "the same id in another tenant is another file")

	require.NoError(t, store.Remove(ctx, "bob", "file-1"))
	assert.Equal(t, "alice data", readAll(t, store, "alice", "file-1"), "deleting one tenant's file leaves the other")
}

func TestFileStoreRejectsIdsThatCouldEscape(t *testing.T) {
	store, root := newTestFileStore(t)
	ctx := context.Background()

	for _, id := range []string{"../escape", "a/b", "", ".", "..", "file 1", "file\x00", strings.Repeat("x", 129)} {
		_, err := store.Create(ctx, "alice", id, strings.NewReader("x"), 1024)
		assert.Error(t, err, "id %q must be refused", id)
		_, err = store.Open(ctx, "alice", id)
		assert.Error(t, err, "id %q must be refused", id)
		assert.Error(t, store.Remove(ctx, "alice", id), "id %q must be refused", id)
	}

	outside := filepath.Join(filepath.Dir(root), "escape")
	_, err := os.Stat(outside)
	assert.True(t, errors.Is(err, os.ErrNotExist), "nothing is written outside the root")
}

func TestFileStoreEnforcesTheLimit(t *testing.T) {
	store, _ := newTestFileStore(t)
	ctx := context.Background()

	_, err := store.Create(ctx, "alice", "file-big", bytes.NewReader(bytes.Repeat([]byte("x"), 100)), 50)
	assert.ErrorIs(t, err, ErrTooLarge)

	_, err = store.Open(ctx, "alice", "file-big")
	assert.ErrorIs(t, err, ErrNotFound, "an oversized upload leaves nothing behind")

	written, err := store.Create(ctx, "alice", "file-exact", bytes.NewReader(bytes.Repeat([]byte("x"), 50)), 50)
	require.NoError(t, err, "a file exactly at the limit is fine")
	assert.Equal(t, int64(50), written)
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("network died") }

func TestFileStoreLeavesNothingBehindOnFailure(t *testing.T) {
	store, root := newTestFileStore(t)
	ctx := context.Background()

	require.NoError(t, func() error {
		_, err := store.Create(ctx, "alice", "file-1", strings.NewReader("good"), 1024)
		return err
	}())

	_, err := store.Create(ctx, "alice", "file-2", failingReader{}, 1024)
	assert.Error(t, err)

	_, err = store.Open(ctx, "alice", "file-2")
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Equal(t, "good", readAll(t, store, "alice", "file-1"), "the other file is untouched")

	var leftovers []string
	require.NoError(t, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.Contains(info.Name(), "file-2") {
			leftovers = append(leftovers, path)
		}
		return nil
	}))
	assert.Empty(t, leftovers, "no temp file is left behind")
}

func TestFileStoreReplacesAtomically(t *testing.T) {
	store, _ := newTestFileStore(t)
	ctx := context.Background()

	_, err := store.Create(ctx, "alice", "file-1", strings.NewReader("first"), 1024)
	require.NoError(t, err)
	_, err = store.Create(ctx, "alice", "file-1", strings.NewReader("second write"), 1024)
	require.NoError(t, err)
	assert.Equal(t, "second write", readAll(t, store, "alice", "file-1"),
		"finalize rewrites the same file, so a replace must be complete or not at all")
}

func TestFileStoreMissingFile(t *testing.T) {
	store, _ := newTestFileStore(t)
	ctx := context.Background()

	_, err := store.Open(ctx, "alice", "file-nope")
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = store.Size(ctx, "alice", "file-nope")
	assert.ErrorIs(t, err, ErrNotFound)
	assert.ErrorIs(t, store.Remove(ctx, "alice", "file-nope"), ErrNotFound)
}

func TestFileStoreWritesGroupReadableFiles(t *testing.T) {
	store, _ := newTestFileStore(t)
	ctx := context.Background()

	_, err := store.Create(ctx, "alice", "file-1", strings.NewReader("data"), 1024)
	require.NoError(t, err)

	path, err := store.path("alice", "file-1")
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(fileMode), info.Mode().Perm(),
		"the volume is shared by replicas running as the same group")
}

func TestFileStoreProbe(t *testing.T) {
	store, root := newTestFileStore(t)
	ctx := context.Background()
	require.NoError(t, store.Probe(ctx))

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	assert.Empty(t, entries, "the probe cleans up after itself")

	if os.Geteuid() == 0 {
		t.Skip("a read-only directory does not stop root")
	}
	readOnly := filepath.Join(t.TempDir(), "ro")
	require.NoError(t, os.Mkdir(readOnly, 0o500))
	assert.Error(t, NewFSStore(filepath.Join(readOnly, "batch")).Probe(ctx),
		"a volume this pod cannot write to must fail at startup, not at the first upload")
}

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
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/volcano-sh/kthena/pkg/kthena-router/common"
)

type testAPI struct {
	service *Service
	files   *FSStore
	root    string
	store   *RedisStore
}

func newTestAPI(t *testing.T) *testAPI {
	t.Helper()
	gin.SetMode(gin.TestMode)
	_, store := newTestStore(t)
	files, root := newTestFileStore(t)
	service := NewService(store, files, Config{MaxFileBytes: 1024, FileExpiration: 720 * time.Hour})
	return &testAPI{service: service, files: files, root: root, store: store}
}

// call runs one request through the same entry point the router hook uses.
func (a *testAPI) call(t *testing.T, tenant string, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	if tenant != "" {
		c.Set(common.UserIdKey, tenant)
	}
	require.True(t, a.service.Handles(req.Method, req.URL.Path), "%s %s must be handled", req.Method, req.URL.Path)
	a.service.Serve(c)
	return rec
}

func uploadRequest(t *testing.T, body string, purposeFirst bool, extra map[string]string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	writePurpose := func() {
		if purpose, ok := extra["purpose"]; ok {
			require.NoError(t, writer.WriteField("purpose", purpose))
		}
	}
	if purposeFirst {
		writePurpose()
	}
	if body != "" {
		part, err := writer.CreateFormFile("file", extra["filename"])
		require.NoError(t, err)
		_, err = io.WriteString(part, body)
		require.NoError(t, err)
	}
	if !purposeFirst {
		writePurpose()
	}
	for key, value := range extra {
		if key == "purpose" || key == "filename" {
			continue
		}
		require.NoError(t, writer.WriteField(key, value))
	}
	require.NoError(t, writer.Close())
	req := httptest.NewRequest(http.MethodPost, filesPath, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out), "body: %s", rec.Body.String())
	return out
}

func mustUpload(t *testing.T, api *testAPI, tenant, body string) string {
	t.Helper()
	rec := api.call(t, tenant, uploadRequest(t, body, true,
		map[string]string{"purpose": PurposeBatch, "filename": "questions.jsonl"}))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	return decode(t, rec)["id"].(string)
}

func TestUploadAndReadBack(t *testing.T) {
	api := newTestAPI(t)
	body := `{"custom_id":"a"}` + "\n"

	rec := api.call(t, "alice", uploadRequest(t, body, true,
		map[string]string{"purpose": PurposeBatch, "filename": "questions.jsonl"}))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	out := decode(t, rec)
	assert.Equal(t, "file", out["object"])
	assert.Equal(t, float64(len(body)), out["bytes"])
	assert.Equal(t, "questions.jsonl", out["filename"])
	assert.Equal(t, PurposeBatch, out["purpose"])
	assert.Equal(t, FileProcessed, out["status"])
	assert.NotZero(t, out["created_at"])
	assert.NotZero(t, out["expires_at"])
	id := out["id"].(string)
	assert.True(t, strings.HasPrefix(id, "file-"), "id is server generated: %s", id)

	rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, filesPath+"/"+id, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, id, decode(t, rec)["id"])

	rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, filesPath+"/"+id+"/content", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, body, rec.Body.String())
}

func TestUploadAcceptsPurposeAfterTheFile(t *testing.T) {
	api := newTestAPI(t)
	rec := api.call(t, "alice", uploadRequest(t, "line\n", false,
		map[string]string{"purpose": PurposeBatch, "filename": "q.jsonl"}))
	assert.Equal(t, http.StatusOK, rec.Code,
		"clients may send the fields in any order, so the file streams first")
}

func TestUploadRejectsTheWrongPurpose(t *testing.T) {
	api := newTestAPI(t)
	rec := api.call(t, "alice", uploadRequest(t, "line\n", true,
		map[string]string{"purpose": "assistants", "filename": "q.jsonl"}))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	out := decode(t, rec)["error"].(map[string]any)
	assert.Equal(t, "purpose", out["param"])
	assert.Contains(t, out["message"], PurposeBatch)

	assert.Empty(t, storedFiles(t, api.root), "the rejected upload leaves no bytes behind")
}

func TestUploadRejectsMissingParts(t *testing.T) {
	api := newTestAPI(t)

	rec := api.call(t, "alice", uploadRequest(t, "", true, map[string]string{"purpose": PurposeBatch}))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "a request with no file part is refused")

	plain := httptest.NewRequest(http.MethodPost, filesPath, strings.NewReader("not multipart"))
	plain.Header.Set("Content-Type", "application/json")
	rec = api.call(t, "alice", plain)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUploadRefusesOversizedFiles(t *testing.T) {
	api := newTestAPI(t)
	rec := api.call(t, "alice", uploadRequest(t, strings.Repeat("x", 2048), true,
		map[string]string{"purpose": PurposeBatch, "filename": "big.jsonl"}))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	assert.Empty(t, storedFiles(t, api.root), "an oversized upload leaves no bytes behind")
}

func TestUploadChecksTheExpiryWindow(t *testing.T) {
	api := newTestAPI(t)
	for _, seconds := range []string{"60", "9999999"} {
		rec := api.call(t, "alice", uploadRequest(t, "line\n", true, map[string]string{
			"purpose": PurposeBatch, "filename": "q.jsonl", "expires_after[seconds]": seconds,
		}))
		assert.Equal(t, http.StatusBadRequest, rec.Code, "seconds=%s is outside 1h..30d", seconds)
	}

	rec := api.call(t, "alice", uploadRequest(t, "line\n", true, map[string]string{
		"purpose": PurposeBatch, "filename": "q.jsonl", "expires_after[seconds]": "7200",
	}))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	out := decode(t, rec)
	assert.InDelta(t, out["created_at"].(float64)+7200, out["expires_at"].(float64), 2)
}

func TestFilenameCannotSteerTheFilesystem(t *testing.T) {
	api := newTestAPI(t)
	rec := api.call(t, "alice", uploadRequest(t, "line\n", true,
		map[string]string{"purpose": PurposeBatch, "filename": "../../etc/passwd"}))
	require.Equal(t, http.StatusOK, rec.Code)

	for _, path := range storedFiles(t, api.root) {
		assert.True(t, strings.HasPrefix(filepath.Base(path), "file-"),
			"stored name comes from our id, not the client: %s", path)
	}
	_, err := os.Stat(filepath.Join(filepath.Dir(api.root), "etc", "passwd"))
	assert.Error(t, err, "nothing is written outside the root")
}

func TestFilesBelongToOneTenant(t *testing.T) {
	api := newTestAPI(t)
	id := mustUpload(t, api, "alice", "line\n")

	for _, path := range []string{filesPath + "/" + id, filesPath + "/" + id + "/content"} {
		rec := api.call(t, "bob", httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusNotFound, rec.Code, "another tenant gets not found for %s", path)
	}
	rec := api.call(t, "bob", httptest.NewRequest(http.MethodDelete, filesPath+"/"+id, nil))
	assert.Equal(t, http.StatusNotFound, rec.Code, "another tenant cannot delete it")

	rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, filesPath+"/"+id, nil))
	assert.Equal(t, http.StatusOK, rec.Code, "the owner is unaffected")
}

func TestDeleteRemovesTheBytes(t *testing.T) {
	api := newTestAPI(t)
	id := mustUpload(t, api, "alice", "line\n")
	require.NotEmpty(t, storedFiles(t, api.root))

	rec := api.call(t, "alice", httptest.NewRequest(http.MethodDelete, filesPath+"/"+id, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	out := decode(t, rec)
	assert.Equal(t, true, out["deleted"])
	assert.Equal(t, id, out["id"])
	assert.Empty(t, storedFiles(t, api.root), "the bytes are gone, not just the record")

	rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, filesPath+"/"+id, nil))
	assert.Equal(t, http.StatusNotFound, rec.Code, "a deleted file is gone from the API")
	rec = api.call(t, "alice", httptest.NewRequest(http.MethodDelete, filesPath+"/"+id, nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDeleteKeepsBytesABatchStillNeeds(t *testing.T) {
	api := newTestAPI(t)
	id := mustUpload(t, api, "alice", "line\n")
	ctx := context.Background()
	require.NoError(t, api.store.client.SAdd(ctx, api.store.fileRefsKey(id), "batch_1").Err())

	rec := api.call(t, "alice", httptest.NewRequest(http.MethodDelete, filesPath+"/"+id, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, storedFiles(t, api.root),
		"a running batch still reads this file, so only the record is hidden")
}

func TestMissingFileAnswersInTheOpenAIShape(t *testing.T) {
	api := newTestAPI(t)
	rec := api.call(t, "alice", httptest.NewRequest(http.MethodGet, filesPath+"/file-nope", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
	out := decode(t, rec)
	errBody, ok := out["error"].(map[string]any)
	require.True(t, ok, "errors use the OpenAI envelope, not the router's bare strings: %s", rec.Body)
	assert.Equal(t, "invalid_request_error", errBody["type"])
	assert.Contains(t, errBody["message"], "file-nope")
}

func TestHandlesOnlyTheBatchRoutes(t *testing.T) {
	api := newTestAPI(t)
	assert.True(t, api.service.Handles(http.MethodPost, filesPath))
	assert.True(t, api.service.Handles(http.MethodGet, filesPath+"/file-1/content"))
	assert.False(t, api.service.Handles(http.MethodPost, "/v1/chat/completions"))
	assert.False(t, api.service.Handles(http.MethodGet, "/v1/models"))
	assert.False(t, api.service.Handles(http.MethodGet, "/v1/filesystem"))

	rec := api.call(t, "alice", httptest.NewRequest(http.MethodPut, filesPath+"/file-1", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code, "an unknown method on a known path is refused")
}

func storedFiles(t *testing.T, root string) []string {
	t.Helper()
	var found []string
	require.NoError(t, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			found = append(found, path)
		}
		return nil
	}))
	return found
}

func TestUploadDoesNotBufferTheFileInMemory(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, store := newTestStore(t)
	files, root := newTestFileStore(t)
	service := NewService(store, files, Config{MaxFileBytes: 64 << 20})

	const size = 8 << 20
	reader, writer := io.Pipe()
	form := multipart.NewWriter(writer)
	go func() {
		defer writer.Close()
		_ = form.WriteField("purpose", PurposeBatch)
		part, err := form.CreateFormFile("file", "big.jsonl")
		if err != nil {
			return
		}
		_, _ = io.CopyN(part, zeroLines{}, size)
		_ = form.Close()
	}()

	req := httptest.NewRequest(http.MethodPost, filesPath, reader)
	req.Header.Set("Content-Type", form.FormDataContentType())
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	c.Set(common.UserIdKey, "alice")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	service.Serve(c)
	runtime.ReadMemStats(&after)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, float64(size), decode(t, rec)["bytes"], "the whole file was stored")

	allocated := after.TotalAlloc - before.TotalAlloc
	assert.Less(t, allocated, uint64(size/4),
		"an %d byte upload allocated %d bytes; it must stream, not buffer", size, allocated)

	stored := storedFiles(t, root)
	require.Len(t, stored, 1)
	info, err := os.Stat(stored[0])
	require.NoError(t, err)
	assert.Equal(t, int64(size), info.Size())
}

type zeroLines struct{}

func (zeroLines) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
		if i%64 == 63 {
			p[i] = '\n'
		}
	}
	return len(p), nil
}

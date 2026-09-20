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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createRequest(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, batchesPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func createBody(fileID string) string {
	return `{"input_file_id":"` + fileID + `","endpoint":"/v1/chat/completions","completion_window":"24h"}`
}

func mustCreateBatch(t *testing.T, api *testAPI, tenant, fileID string) map[string]any {
	t.Helper()
	rec := api.call(t, tenant, createRequest(createBody(fileID)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	return decode(t, rec)
}

func TestCreateBatchFromAnUploadedFile(t *testing.T) {
	api := newTestAPI(t)
	fileID := mustUpload(t, api, "alice", `{"custom_id":"a"}`+"\n")

	rec := api.call(t, "alice", createRequest(
		`{"input_file_id":"`+fileID+`","endpoint":"/v1/chat/completions","completion_window":"24h",`+
			`"metadata":{"team":"search"}}`))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	out := decode(t, rec)

	assert.Equal(t, "batch", out["object"])
	assert.True(t, strings.HasPrefix(out["id"].(string), "batch_"), "id is server generated: %v", out["id"])
	assert.Equal(t, string(StatusValidating), out["status"], "a new batch waits for a worker")
	assert.Equal(t, fileID, out["input_file_id"])
	assert.Equal(t, "/v1/chat/completions", out["endpoint"])
	assert.Equal(t, "24h", out["completion_window"])
	assert.Equal(t, map[string]any{"team": "search"}, out["metadata"])
	counts := out["request_counts"].(map[string]any)
	assert.Equal(t, float64(0), counts["total"])
	assert.InDelta(t, out["created_at"].(float64)+86400, out["expires_at"].(float64), 2,
		"the completion window is 24 hours")
	assert.NotContains(t, out, "completed_at", "timestamps that have not happened are left out")

	job, err := api.store.GetJob(context.Background(), out["id"].(string))
	require.NoError(t, err)
	assert.Equal(t, "alice", job.Tenant)
}

func TestCreateBatchHoldsTheInputFile(t *testing.T) {
	api := newTestAPI(t)
	fileID := mustUpload(t, api, "alice", "line\n")
	mustCreateBatch(t, api, "alice", fileID)

	rec := api.call(t, "alice", httptest.NewRequest(http.MethodDelete, filesPath+"/"+fileID, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, storedFiles(t, api.root),
		"deleting the input file while a batch needs it must keep the bytes")
}

func TestCreateBatchChecksItsInput(t *testing.T) {
	api := newTestAPI(t)
	fileID := mustUpload(t, api, "alice", "line\n")

	cases := []struct {
		name  string
		body  string
		param string
	}{
		{"no input file", `{"endpoint":"/v1/chat/completions","completion_window":"24h"}`, "input_file_id"},
		{"unknown file", createBody("file-nope"), "input_file_id"},
		{"unsupported endpoint", `{"input_file_id":"` + fileID + `","endpoint":"/v1/embeddings","completion_window":"24h"}`, "endpoint"},
		{"missing endpoint", `{"input_file_id":"` + fileID + `","completion_window":"24h"}`, "endpoint"},
		{"wrong window", `{"input_file_id":"` + fileID + `","endpoint":"/v1/chat/completions","completion_window":"1h"}`, "completion_window"},
		{"unknown field", `{"input_file_id":"` + fileID + `","endpoint":"/v1/chat/completions","completion_window":"24h","oops":1}`, ""},
		{"not json", `not json`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := api.call(t, "alice", createRequest(tc.body))
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			if tc.param != "" {
				body := decode(t, rec)["error"].(map[string]any)
				assert.Equal(t, tc.param, body["param"])
			}
		})
	}
}

func TestCreateBatchChecksMetadataLimits(t *testing.T) {
	api := newTestAPI(t)
	fileID := mustUpload(t, api, "alice", "line\n")

	pairs := make([]string, 0, 17)
	for i := 0; i < 17; i++ {
		pairs = append(pairs, `"k`+strings.Repeat("0", 2)+string(rune('a'+i))+`":"v"`)
	}
	tooMany := `{"input_file_id":"` + fileID + `","endpoint":"/v1/chat/completions","completion_window":"24h",` +
		`"metadata":{` + strings.Join(pairs, ",") + `}}`
	rec := api.call(t, "alice", createRequest(tooMany))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "at most 16 metadata pairs")

	longKey := `{"input_file_id":"` + fileID + `","endpoint":"/v1/chat/completions","completion_window":"24h",` +
		`"metadata":{"` + strings.Repeat("k", 65) + `":"v"}}`
	rec = api.call(t, "alice", createRequest(longKey))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "keys are at most 64 characters")

	longValue := `{"input_file_id":"` + fileID + `","endpoint":"/v1/chat/completions","completion_window":"24h",` +
		`"metadata":{"k":"` + strings.Repeat("v", 513) + `"}}`
	rec = api.call(t, "alice", createRequest(longValue))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "values are at most 512 characters")
}

func TestCreateBatchRefusesAnotherTenantsFile(t *testing.T) {
	api := newTestAPI(t)
	fileID := mustUpload(t, api, "alice", "line\n")

	rec := api.call(t, "bob", createRequest(createBody(fileID)))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "bob cannot batch alice's file")
	assert.Contains(t, rec.Body.String(), "no such file")
}

func TestCreateBatchRefusesAnOutputFile(t *testing.T) {
	api := newTestAPI(t)
	ctx := context.Background()
	require.NoError(t, api.store.CreateFile(ctx, &File{
		ID: "file-result", Tenant: "alice", Purpose: PurposeBatchOutput, Status: FileProcessed,
	}))

	rec := api.call(t, "alice", createRequest(createBody("file-result")))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), PurposeBatch)
}

func TestCreateBatchRecordsHowTheRequestArrived(t *testing.T) {
	api := newTestAPI(t)
	api.service.config.ForwardHeaders = []string{"X-Tier"}
	api.service.config.GatewayContext = func(c *gin.Context) Dispatch {
		return Dispatch{GatewayKey: "default/gw1", ListenerName: "http", ListenerPort: 8080}
	}
	fileID := mustUpload(t, api, "alice", "line\n")

	req := createRequest(createBody(fileID))
	req.Host = "api.example.com"
	req.Header.Set("X-Tier", "gold")
	req.Header.Set("X-Secret", "do-not-store")
	rec := api.call(t, "alice", req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	job, err := api.store.GetJob(context.Background(), decode(t, rec)["id"].(string))
	require.NoError(t, err)
	assert.Equal(t, "default/gw1", job.Dispatch.GatewayKey, "gateway mode has no default listener to fall back on")
	assert.Equal(t, "http", job.Dispatch.ListenerName)
	assert.Equal(t, 8080, job.Dispatch.ListenerPort)
	assert.Equal(t, "api.example.com", job.Dispatch.Host, "HTTPRoute matching reads the Host")
	assert.Equal(t, map[string]string{"X-Tier": "gold"}, job.Dispatch.Headers)
	assert.NotContains(t, job.Dispatch.Headers, "X-Secret", "only allowlisted headers are kept")
}

func TestGetAndCancelBatch(t *testing.T) {
	api := newTestAPI(t)
	fileID := mustUpload(t, api, "alice", "line\n")
	id := mustCreateBatch(t, api, "alice", fileID)["id"].(string)

	rec := api.call(t, "alice", httptest.NewRequest(http.MethodGet, batchesPath+"/"+id, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, id, decode(t, rec)["id"])

	rec = api.call(t, "alice", httptest.NewRequest(http.MethodPost, batchesPath+"/"+id+"/cancel", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	out := decode(t, rec)
	assert.Equal(t, string(StatusCancelled), out["status"], "a batch nobody started yet cancels at once")
	assert.NotZero(t, out["cancelled_at"])

	rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, batchesPath+"/"+id, nil))
	assert.Equal(t, string(StatusCancelled), decode(t, rec)["status"], "the cancel stuck")
}

func TestCancelReportsTheRunningBatchAsCancelling(t *testing.T) {
	api := newTestAPI(t)
	ctx := context.Background()
	fileID := mustUpload(t, api, "alice", "line\n")
	id := mustCreateBatch(t, api, "alice", fileID)["id"].(string)

	lease, err := api.store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NoError(t, api.store.Transition(ctx, lease, StatusValidating, StatusInProgress, nil))

	rec := api.call(t, "alice", httptest.NewRequest(http.MethodPost, batchesPath+"/"+id+"/cancel", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, string(StatusCancelling), decode(t, rec)["status"],
		"a running batch stops at its own pace, keeping the answers it already has")
}

func TestBatchesBelongToOneTenant(t *testing.T) {
	api := newTestAPI(t)
	fileID := mustUpload(t, api, "alice", "line\n")
	id := mustCreateBatch(t, api, "alice", fileID)["id"].(string)

	rec := api.call(t, "bob", httptest.NewRequest(http.MethodGet, batchesPath+"/"+id, nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	rec = api.call(t, "bob", httptest.NewRequest(http.MethodPost, batchesPath+"/"+id+"/cancel", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = api.call(t, "bob", httptest.NewRequest(http.MethodGet, batchesPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, decode(t, rec)["data"], "bob sees none of alice's batches")

	job, err := api.store.GetJob(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, StatusValidating, job.Status, "bob's cancel attempt changed nothing")
}

func TestListBatchesPages(t *testing.T) {
	api := newTestAPI(t)
	fileID := mustUpload(t, api, "alice", "line\n")
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		ids = append(ids, mustCreateBatch(t, api, "alice", fileID)["id"].(string))
	}

	rec := api.call(t, "alice", httptest.NewRequest(http.MethodGet, batchesPath+"?limit=2", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	out := decode(t, rec)
	assert.Equal(t, "list", out["object"])
	assert.Equal(t, true, out["has_more"])
	data := out["data"].([]any)
	require.Len(t, data, 2)
	assert.Equal(t, out["first_id"], data[0].(map[string]any)["id"])
	assert.Equal(t, out["last_id"], data[1].(map[string]any)["id"])

	rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, batchesPath+"?limit=2&after="+out["last_id"].(string), nil))
	require.Equal(t, http.StatusOK, rec.Code)
	out = decode(t, rec)
	assert.Equal(t, false, out["has_more"], "the last page says so")
	require.Len(t, out["data"].([]any), 1)

	for _, query := range []string{"?limit=0", "?limit=101", "?limit=many"} {
		rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, batchesPath+query, nil))
		assert.Equal(t, http.StatusBadRequest, rec.Code, "limit %s is out of range", query)
	}
	rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, batchesPath+"?after=batch_nope", nil))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "an unknown cursor is refused")
	_ = ids
}

func TestListFilesEndpoint(t *testing.T) {
	api := newTestAPI(t)
	first := mustUpload(t, api, "alice", "one\n")
	second := mustUpload(t, api, "alice", "two\n")
	mustUpload(t, api, "bob", "three\n")

	rec := api.call(t, "alice", httptest.NewRequest(http.MethodGet, filesPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	out := decode(t, rec)
	data := out["data"].([]any)
	require.Len(t, data, 2, "only alice's files")
	assert.Equal(t, second, data[0].(map[string]any)["id"], "newest first")
	assert.Equal(t, first, data[1].(map[string]any)["id"])

	rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, filesPath+"?purpose="+PurposeBatchOutput, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, decode(t, rec)["data"], "no results have been produced yet")

	rec = api.call(t, "alice", httptest.NewRequest(http.MethodDelete, filesPath+"/"+second, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	rec = api.call(t, "alice", httptest.NewRequest(http.MethodGet, filesPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, decode(t, rec)["data"], 1, "a deleted file leaves the listing")
}

func TestUnknownBatchRoutesAreRefused(t *testing.T) {
	api := newTestAPI(t)
	for _, tc := range []struct {
		method, path string
	}{
		{http.MethodPut, batchesPath},
		{http.MethodDelete, batchesPath + "/batch_1"},
		{http.MethodGet, batchesPath + "/batch_1/cancel"},
		{http.MethodGet, batchesPath + "/batch_1/extra/deep"},
	} {
		rec := api.call(t, "alice", httptest.NewRequest(tc.method, tc.path, nil))
		assert.Equal(t, http.StatusNotFound, rec.Code, "%s %s", tc.method, tc.path)
	}
	assert.False(t, api.service.Handles(http.MethodGet, "/v1/batchesomething"))
}

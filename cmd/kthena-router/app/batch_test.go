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

package app

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/volcano-sh/kthena/pkg/kthena-router/batch"
	"github.com/volcano-sh/kthena/pkg/kthena-router/common"
	"github.com/volcano-sh/kthena/pkg/kthena-router/router"
)

func TestBatchOptionsDefaults(t *testing.T) {
	options := batchOptionsFromEnv()

	assert.False(t, options.enabled, "batch must be off unless an operator turns it on")
	assert.True(t, options.workerEnabled)
	assert.Equal(t, defaultBatchRoot, options.root)
	assert.Equal(t, int64(200<<20), options.maxFileBytes, "200MB, the OpenAI limit")
	assert.Equal(t, int64(50000), options.maxRequests)
	assert.Equal(t, defaultBatchLeaseTTL, options.leaseTTL)
	assert.Equal(t, defaultBatchLineTimeout, options.lineTimeout)
	assert.Equal(t, 2, options.maxConcurrentJobs)
	assert.True(t, options.accessLog)
	assert.Empty(t, options.forwardHeaders)
}

func TestBatchOptionsFromTheEnvironment(t *testing.T) {
	t.Setenv("BATCH_ENABLED", "true")
	t.Setenv("BATCH_WORKER_ENABLED", "false")
	t.Setenv("BATCH_STORAGE_PATH", "/mnt/batch")
	t.Setenv("BATCH_MAX_FILE_BYTES", "1024")
	t.Setenv("BATCH_MAX_REQUESTS", "10")
	t.Setenv("BATCH_LEASE_TTL", "45s")
	t.Setenv("BATCH_LINE_TIMEOUT", "2m")
	t.Setenv("BATCH_MAX_CONCURRENT_JOBS", "4")
	t.Setenv("BATCH_CONCURRENCY", "16")
	t.Setenv("BATCH_ACCESS_LOG", "false")
	t.Setenv("BATCH_FORWARD_HEADERS", " X-Tier , X-Team ,")

	options := batchOptionsFromEnv()
	assert.True(t, options.enabled)
	assert.False(t, options.workerEnabled)
	assert.Equal(t, "/mnt/batch", options.root)
	assert.Equal(t, int64(1024), options.maxFileBytes)
	assert.Equal(t, int64(10), options.maxRequests)
	assert.Equal(t, 45*time.Second, options.leaseTTL)
	assert.Equal(t, 2*time.Minute, options.lineTimeout)
	assert.Equal(t, 4, options.maxConcurrentJobs)
	assert.Equal(t, 16, options.concurrency)
	assert.False(t, options.accessLog)
	assert.Equal(t, []string{"X-Tier", "X-Team"}, options.forwardHeaders, "blanks are dropped")
}

func TestBatchOptionsIgnoreRubbish(t *testing.T) {
	t.Setenv("BATCH_ENABLED", "yes please")
	t.Setenv("BATCH_MAX_FILE_BYTES", "-5")
	t.Setenv("BATCH_LEASE_TTL", "soon")
	t.Setenv("BATCH_MAX_CONCURRENT_JOBS", "0")

	options := batchOptionsFromEnv()
	assert.False(t, options.enabled, "a value nobody can parse must not turn the feature on")
	assert.Equal(t, int64(200<<20), options.maxFileBytes)
	assert.Equal(t, defaultBatchLeaseTTL, options.leaseTTL)
	assert.Equal(t, 2, options.maxConcurrentJobs)
}

// callIdentity runs one request through the identity middleware and reports what the
// router would see afterwards.
func callIdentity(t *testing.T, ctx context.Context) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"qwen"}`)).WithContext(ctx)
	batchIdentity()(c)
	return recorder, c
}

func TestBatchIdentityRefusesRequestsWithoutOne(t *testing.T) {
	recorder, c := callIdentity(t, context.Background())

	assert.Equal(t, http.StatusForbidden, recorder.Code,
		"the batch engine is in-process only; nothing without a batch identity may use it")
	assert.True(t, c.IsAborted())
	assert.Empty(t, c.GetString(common.UserIdKey))
}

func TestBatchIdentityReplaysTheOwnerAndGateway(t *testing.T) {
	ctx := batch.WithIdentity(context.Background(), batch.Identity{
		Tenant: "alice",
		Dispatch: batch.Dispatch{
			GatewayKey:   "default/gw1",
			ListenerName: "http",
			ListenerPort: 8080,
		},
	})
	recorder, c := callIdentity(t, ctx)

	assert.NotEqual(t, http.StatusForbidden, recorder.Code)
	assert.False(t, c.IsAborted())
	assert.Equal(t, "alice", c.GetString(common.UserIdKey),
		"fairness and token accounting charge the owner of the batch")
	assert.Equal(t, "default/gw1", c.GetString(router.GatewayKey))
	assert.Equal(t, "http", c.GetString(router.GatewayListenerNameKey))
	assert.Equal(t, 8080, c.GetInt(router.GatewayListenerPortKey))
}

func TestBatchIdentityWithoutAGatewayLeavesTheKeysAlone(t *testing.T) {
	ctx := batch.WithIdentity(context.Background(), batch.Identity{Tenant: "alice"})
	_, c := callIdentity(t, ctx)

	assert.Equal(t, "alice", c.GetString(common.UserIdKey))
	_, hasGateway := c.Get(router.GatewayKey)
	assert.False(t, hasGateway, "without Gateway API there is nothing to replay")
}

func TestGatewayContextIsCapturedFromTheCreatingRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/batches", nil)
	c.Request.Host = "api.example.com"
	c.Set(router.GatewayKey, "default/gw1")
	c.Set(router.GatewayListenerNameKey, "http")
	c.Set(router.GatewayListenerPortKey, 8080)

	dispatch := gatewayContextOf(c)
	assert.Equal(t, "default/gw1", dispatch.GatewayKey)
	assert.Equal(t, "http", dispatch.ListenerName)
	assert.Equal(t, 8080, dispatch.ListenerPort)
	assert.Equal(t, "api.example.com", dispatch.Host)
}

func TestGatewayContextWithoutGatewayAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/batches", nil)
	c.Request.Host = "kthena-router:8080"

	dispatch := gatewayContextOf(c)
	assert.Empty(t, dispatch.GatewayKey)
	assert.Equal(t, "kthena-router:8080", dispatch.Host, "the Host is still worth keeping")
}

func TestBatchStaysOffUntilItIsAskedFor(t *testing.T) {
	server := &Server{}
	gwRouter := &router.Router{}

	server.startBatch(context.Background(), gwRouter)

	assert.Nil(t, gwRouter.APIExtension(), "no batch endpoints exist unless batch is enabled")
}

func TestBatchRefusesToStartWithoutRedis(t *testing.T) {
	t.Setenv("BATCH_ENABLED", "true")
	t.Setenv("BATCH_WORKER_ENABLED", "false")
	// The volume is fine, so Redis is the only thing that can stop the startup here.
	t.Setenv("BATCH_STORAGE_PATH", t.TempDir())
	t.Setenv("REDIS_HOST", "127.0.0.1")
	t.Setenv("REDIS_PORT", "1")

	server := &Server{}
	gwRouter := &router.Router{}
	server.startBatch(context.Background(), gwRouter)

	assert.Nil(t, gwRouter.APIExtension(),
		"half-started is worse than off: batch needs Redis, so without it the endpoints stay away")
}

// withTestRedis points the router at a Redis the startup checks can reach: miniredis by
// default, so these tests run anywhere, or a real one when KTHENA_TEST_REDIS_ADDR is set.
func withTestRedis(t *testing.T) {
	t.Helper()
	addr := os.Getenv("KTHENA_TEST_REDIS_ADDR")
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	}
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	t.Setenv("REDIS_HOST", host)
	t.Setenv("REDIS_PORT", port)
}

func TestBatchRefusesToStartWithoutWritableStorage(t *testing.T) {
	// Redis is reachable here, so the volume is the only thing that can refuse.
	withTestRedis(t)
	t.Setenv("BATCH_ENABLED", "true")
	t.Setenv("BATCH_WORKER_ENABLED", "false")
	t.Setenv("BATCH_STORAGE_PATH", "/proc/kthena-batch-cannot-write-here")

	server := &Server{}
	gwRouter := &router.Router{}
	server.startBatch(context.Background(), gwRouter)

	assert.Nil(t, gwRouter.APIExtension(),
		"a volume this pod cannot write to must stop batch at startup, not at the first upload")
}

func TestBatchStartsWhenRedisAndStorageAreReady(t *testing.T) {
	withTestRedis(t)
	t.Setenv("BATCH_ENABLED", "true")
	t.Setenv("BATCH_WORKER_ENABLED", "false")
	t.Setenv("BATCH_STORAGE_PATH", t.TempDir())

	server := &Server{}
	gwRouter := &router.Router{}
	server.startBatch(context.Background(), gwRouter)

	extension := gwRouter.APIExtension()
	require.NotNil(t, extension, "with Redis and a writable volume the batch endpoints appear")
	assert.True(t, extension.Handles(http.MethodPost, "/v1/batches"))
	assert.True(t, extension.Handles(http.MethodPost, "/v1/files"))
	assert.False(t, extension.Handles(http.MethodPost, "/v1/chat/completions"),
		"ordinary model requests still go the normal way")
}

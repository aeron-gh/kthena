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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// routerLike stands in for the router's own handler chain: a recovery middleware, a
// middleware that trusts only the batch identity in the context, and a catch-all.
func routerLike(t *testing.T, handle gin.HandlerFunc) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	gin.DefaultErrorWriter = io.Discard
	engine := gin.New()
	engine.Use(gin.CustomRecovery(func(c *gin.Context, recovered any) {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprint(recovered)})
	}))
	engine.Use(func(c *gin.Context) {
		if identity, ok := IdentityFromContext(c.Request.Context()); ok {
			c.Set("user_id", identity.Tenant)
			c.Set("gatewayKey", identity.Dispatch.GatewayKey)
			c.Set("gatewayListenerName", identity.Dispatch.ListenerName)
			c.Set("gatewayListenerPort", identity.Dispatch.ListenerPort)
		}
		c.Next()
	})
	engine.Any("/*path", handle)
	return engine
}

func batchRequest() *Request {
	return &Request{
		JobID:     "batch_1",
		Tenant:    "alice",
		RequestID: "batch_req_abc",
		Endpoint:  testEndpoint,
		Dispatch: Dispatch{
			GatewayKey:   "default/gw1",
			ListenerName: "http",
			ListenerPort: 8080,
			Host:         "api.example.com",
			Headers:      map[string]string{"X-Tier": "gold"},
		},
		Body: []byte(`{"model":"qwen","messages":[{"role":"user","content":"hi"}]}`),
	}
}

func TestDispatchReplaysWhoTheBatchBelongsTo(t *testing.T) {
	handler := routerLike(t, func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		c.JSON(http.StatusOK, gin.H{
			"user":         c.GetString("user_id"),
			"gateway":      c.GetString("gatewayKey"),
			"listener":     c.GetString("gatewayListenerName"),
			"port":         c.GetInt("gatewayListenerPort"),
			"host":         c.Request.Host,
			"path":         c.Request.URL.Path,
			"method":       c.Request.Method,
			"content_type": c.GetHeader("Content-Type"),
			"request_id":   c.GetHeader("X-Request-ID"),
			"tier":         c.GetHeader("X-Tier"),
			"body":         string(body),
		})
	})
	dispatcher := NewHandlerDispatcher(handler, 1<<20, time.Minute)

	response := dispatcher.Dispatch(context.Background(), batchRequest())
	require.NotNil(t, response)
	require.NoError(t, response.Err)
	assert.Equal(t, http.StatusOK, response.StatusCode)

	var seen map[string]any
	require.NoError(t, json.Unmarshal(response.Body, &seen))
	assert.Equal(t, "alice", seen["user"], "the owner follows the line, so fairness and usage are charged to them")
	assert.Equal(t, "default/gw1", seen["gateway"], "gateway mode has no default listener to fall back on")
	assert.Equal(t, "http", seen["listener"])
	assert.Equal(t, float64(8080), seen["port"])
	assert.Equal(t, "api.example.com", seen["host"], "HTTPRoute matching reads the Host")
	assert.Equal(t, testEndpoint, seen["path"])
	assert.Equal(t, http.MethodPost, seen["method"])
	assert.Equal(t, "application/json", seen["content_type"])
	assert.Equal(t, "batch_req_abc", seen["request_id"], "the router logs the id the output file will show")
	assert.Equal(t, "gold", seen["tier"], "allowlisted headers are replayed for header-matched routes")
	assert.JSONEq(t, `{"model":"qwen","messages":[{"role":"user","content":"hi"}]}`, seen["body"].(string))
}

func TestDispatchIdentityCannotBeForged(t *testing.T) {
	handler := routerLike(t, func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"user": c.GetString("user_id"), "gateway": c.GetString("gatewayKey")})
	})
	dispatcher := NewHandlerDispatcher(handler, 1<<20, time.Minute)

	request := batchRequest()
	request.Tenant = "alice"
	request.Dispatch.Headers = map[string]string{
		"X-Batch-Tenant": "mallory",
		"X-User-Id":      "mallory",
	}
	response := dispatcher.Dispatch(context.Background(), request)
	require.NoError(t, response.Err)

	var seen map[string]any
	require.NoError(t, json.Unmarshal(response.Body, &seen))
	assert.Equal(t, "alice", seen["user"],
		"the identity comes from the context, so headers cannot change whose batch this is")
}

func TestDispatchSurvivesAStreamingHandler(t *testing.T) {
	handler := routerLike(t, func(c *gin.Context) {
		chunks := 0
		c.Stream(func(w io.Writer) bool {
			chunks++
			fmt.Fprintf(w, "chunk%d;", chunks)
			return chunks < 3
		})
	})
	dispatcher := NewHandlerDispatcher(handler, 1<<20, time.Minute)

	response := dispatcher.Dispatch(context.Background(), batchRequest())
	require.NoError(t, response.Err, "gin's Stream casts the writer to http.CloseNotifier")
	assert.Equal(t, "chunk1;chunk2;chunk3;", string(response.Body))
}

func TestDispatchContainsAPanic(t *testing.T) {
	handler := routerLike(t, func(c *gin.Context) { panic("boom inside the router") })
	dispatcher := NewHandlerDispatcher(handler, 1<<20, time.Minute)

	response := dispatcher.Dispatch(context.Background(), batchRequest())
	require.NotNil(t, response)
	assert.Equal(t, http.StatusInternalServerError, response.StatusCode,
		"one bad line must not take the router down with it")
	assert.Contains(t, string(response.Body), "boom inside the router")
}

// rawPanicHandler panics outside any gin recovery, the way a bug in a plain handler would.
type rawPanicHandler struct{}

func (rawPanicHandler) ServeHTTP(http.ResponseWriter, *http.Request) { panic("no recovery here") }

func TestDispatchContainsAPanicWithoutGinRecovery(t *testing.T) {
	dispatcher := NewHandlerDispatcher(rawPanicHandler{}, 1<<20, time.Minute)

	response := dispatcher.Dispatch(context.Background(), batchRequest())
	require.NotNil(t, response)
	require.Error(t, response.Err, "the worker goroutine must not die with the handler")
	assert.Contains(t, response.Err.Error(), "no recovery here")
}

func TestDispatchRefusesAnOversizedAnswer(t *testing.T) {
	handler := routerLike(t, func(c *gin.Context) {
		c.Data(http.StatusOK, "application/json", []byte(`{"text":"`+strings.Repeat("x", 4096)+`"}`))
	})
	dispatcher := NewHandlerDispatcher(handler, 256, time.Minute)

	response := dispatcher.Dispatch(context.Background(), batchRequest())
	require.NotNil(t, response)
	assert.ErrorIs(t, response.Err, ErrResponseTooLarge,
		"the router has a 512Mi limit, so one huge answer must not be buffered whole")
	assert.Empty(t, response.Body)
}

func TestDispatchStopsASlowLine(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	handler := routerLike(t, func(c *gin.Context) {
		select {
		case <-c.Request.Context().Done():
		case <-release:
		}
		c.Status(http.StatusOK)
	})
	dispatcher := NewHandlerDispatcher(handler, 1<<20, 50*time.Millisecond)

	start := time.Now()
	response := dispatcher.Dispatch(context.Background(), batchRequest())
	assert.Less(t, time.Since(start), 5*time.Second, "a stuck model must not hold a worker slot forever")
	require.NotNil(t, response)
}

func TestDispatchPassesTheCallersCancel(t *testing.T) {
	started := make(chan struct{})
	handler := routerLike(t, func(c *gin.Context) {
		close(started)
		<-c.Request.Context().Done()
		c.Status(http.StatusOK)
	})
	dispatcher := NewHandlerDispatcher(handler, 1<<20, time.Minute)

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan *Response, 1)
	go func() { done <- dispatcher.Dispatch(ctx, batchRequest()) }()
	<-started
	stop()

	select {
	case response := <-done:
		require.NotNil(t, response, "shutdown must reach the in-flight line")
	case <-time.After(5 * time.Second):
		t.Fatal("the line ignored the shutdown")
	}
}

func TestDispatchLeavesNoGoroutinesBehind(t *testing.T) {
	handler := routerLike(t, func(c *gin.Context) {
		chunks := 0
		c.Stream(func(w io.Writer) bool {
			chunks++
			fmt.Fprint(w, "x")
			return chunks < 2
		})
	})
	dispatcher := NewHandlerDispatcher(handler, 1<<20, time.Minute)

	for i := 0; i < 20; i++ {
		require.NoError(t, dispatcher.Dispatch(context.Background(), batchRequest()).Err)
	}
	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < 200; i++ {
		require.NoError(t, dispatcher.Dispatch(context.Background(), batchRequest()).Err)
	}
	waitFor(t, 5*time.Second, "the CloseNotify goroutines to finish", func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= before+10
	})
}

func TestBatchRunsThroughTheRealHandlerChain(t *testing.T) {
	f := newExecFixture(t, 6, ExecConfig{Concurrency: 2, CheckpointEvery: 2, MaxResponseBytes: 1 << 20})
	handler := routerLike(t, func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		var parsed struct {
			CustomID string `json:"custom_id"`
		}
		require.NoError(t, json.Unmarshal(body, &parsed))
		assert.Equal(t, "alice", c.GetString("user_id"), "every line carries its owner")
		c.JSON(http.StatusOK, gin.H{
			"id":      "chatcmpl-" + parsed.CustomID,
			"choices": []gin.H{{"message": gin.H{"content": "answer for " + parsed.CustomID}}},
			"usage":   gin.H{"prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8},
		})
	})
	f.exec.dispatcher = NewHandlerDispatcher(handler, 1<<20, time.Minute)

	ctx := context.Background()
	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, job.Status)
	assert.Equal(t, Counts{Total: 6, Completed: 6}, job.Counts)

	lines := f.results(t, job.OutputFileID)
	require.Len(t, lines, 6)
	for _, line := range lines {
		require.NotNil(t, line.Response)
		assert.Equal(t, 200, line.Response.StatusCode)
		assert.Contains(t, string(line.Response.Body), "answer for "+line.CustomID)
	}
	checkpoint, err := f.store.GetCheckpoint(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, int64(48), checkpoint.Usage.TotalTokens, "usage comes back from the real answers")
}

func TestBatchRecordsAnOversizedAnswerAsAFailedLine(t *testing.T) {
	f := newExecFixture(t, 3, ExecConfig{Concurrency: 1, MaxResponseBytes: 512})
	handler := routerLike(t, func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		var parsed struct {
			CustomID string `json:"custom_id"`
		}
		require.NoError(t, json.Unmarshal(body, &parsed))
		if parsed.CustomID == "req-001" {
			c.Data(http.StatusOK, "application/json", []byte(`{"text":"`+strings.Repeat("x", 4096)+`"}`))
			return
		}
		c.JSON(http.StatusOK, gin.H{"id": "chatcmpl-" + parsed.CustomID})
	})
	f.exec.dispatcher = NewHandlerDispatcher(handler, 512, time.Minute)

	ctx := context.Background()
	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, job.Status, "one huge answer must not sink the whole batch")
	assert.Equal(t, Counts{Total: 3, Completed: 2, Failed: 1}, job.Counts)

	failures := f.results(t, job.ErrorFileID)
	require.Len(t, failures, 1)
	assert.Equal(t, "req-001", failures[0].CustomID)
	require.NotNil(t, failures[0].Error)
	assert.Equal(t, "response_too_large", failures[0].Error.Code,
		"the user is told why that request has no answer")
}

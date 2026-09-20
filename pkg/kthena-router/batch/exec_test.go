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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func redisZ(score float64, member string) redis.Z {
	return redis.Z{Score: score, Member: member}
}

// fakeDispatcher stands in for the router until the real dispatch path is wired up.
type fakeDispatcher struct {
	mu       sync.Mutex
	seen     []string
	calls    atomic.Int64
	respond  func(customID string, call int64) *Response
	onCall   func(call int64)
	failures map[string]int
}

func (d *fakeDispatcher) Dispatch(ctx context.Context, request *Request) *Response {
	call := d.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return &Response{Err: err}
	}
	customID := customIDOf(request.Body)
	var body struct {
		CustomID string `json:"custom_id"`
	}
	_ = json.Unmarshal(request.Body, &body)
	if body.CustomID != "" {
		customID = body.CustomID
	}
	d.mu.Lock()
	d.seen = append(d.seen, customID)
	d.mu.Unlock()
	if d.onCall != nil {
		d.onCall(call)
	}
	if d.respond != nil {
		return d.respond(customID, call)
	}
	return okResponse(customID)
}

func (d *fakeDispatcher) dispatched() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.seen...)
}

func okResponse(customID string) *Response {
	body := fmt.Sprintf(`{"id":"chatcmpl-%s","choices":[{"message":{"content":"answer for %s"}}],`+
		`"usage":{"prompt_tokens":3,"completion_tokens":5,"total_tokens":8}}`, customID, customID)
	return &Response{StatusCode: 200, Body: []byte(body)}
}

type execFixture struct {
	store      *RedisStore
	files      *FSStore
	dispatcher *fakeDispatcher
	exec       *Executor
	job        *Job
	server     *testServer
}

// batchLine is one request in the input file; the body carries the custom_id too, so
// the fake dispatcher can tell which request it was handed.
func batchLine(id string) string {
	return `{"custom_id":"` + id + `","method":"POST","url":"` + testEndpoint +
		`","body":{"model":"qwen","custom_id":"` + id + `","messages":[{"role":"user","content":"hi"}]}}`
}

func newExecFixture(t *testing.T, requests int, config ExecConfig) *execFixture {
	t.Helper()
	server, store := newTestStore(t)
	files, _ := newTestFileStore(t)
	ctx := context.Background()

	var content strings.Builder
	for i := 0; i < requests; i++ {
		content.WriteString(batchLine(fmt.Sprintf("req-%03d", i)) + "\n")
	}
	_, err := files.Create(ctx, "alice", "file-input", strings.NewReader(content.String()), 0)
	require.NoError(t, err)
	require.NoError(t, store.CreateFile(ctx, &File{
		ID: "file-input", Tenant: "alice", Purpose: PurposeBatch, Status: FileProcessed,
	}))

	job := &Job{
		ID: "batch_1", Tenant: "alice", Endpoint: testEndpoint, InputFileID: "file-input",
		CompletionWindow: "24h", ExpiresAt: time.Now().Add(24 * time.Hour).Unix(),
	}
	require.NoError(t, store.CreateJob(ctx, job))
	require.NoError(t, store.HoldFile(ctx, "file-input", job.ID))

	dispatcher := &fakeDispatcher{failures: map[string]int{}}
	return &execFixture{
		store: store, files: files, dispatcher: dispatcher, job: job, server: server,
		exec: NewExecutor(store, files, dispatcher, config),
	}
}

func (f *execFixture) claim(t *testing.T, holder string) *Lease {
	t.Helper()
	lease, err := f.store.Claim(context.Background(), holder, testLeaseTTL)
	require.NoError(t, err)
	require.NotNil(t, lease, "there should be a batch to claim")
	return lease
}

// results reads back one of the produced files as decoded output lines.
func (f *execFixture) results(t *testing.T, fileID string) []outputLine {
	t.Helper()
	if fileID == "" {
		return nil
	}
	file, err := f.files.Open(context.Background(), "alice", fileID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	require.NoError(t, err)
	defer file.Close()

	var lines []outputLine
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		var line outputLine
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line), "line: %s", scanner.Text())
		lines = append(lines, line)
	}
	require.NoError(t, scanner.Err())
	return lines
}

func customIDs(lines []outputLine) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, line.CustomID)
	}
	return out
}

func TestExecutorRunsABatchEndToEnd(t *testing.T) {
	f := newExecFixture(t, 5, ExecConfig{Concurrency: 2, CheckpointEvery: 2})
	ctx := context.Background()
	lease := f.claim(t, "router-0")

	require.NoError(t, f.exec.Run(ctx, lease))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, job.Status)
	assert.Equal(t, Counts{Total: 5, Completed: 5, Failed: 0}, job.Counts)
	assert.NotZero(t, job.CompletedAt)
	require.NotEmpty(t, job.OutputFileID, "a finished batch has an output file")
	assert.Empty(t, job.ErrorFileID, "nothing failed, so there is no error file")

	lines := f.results(t, job.OutputFileID)
	require.Len(t, lines, 5)
	assert.ElementsMatch(t,
		[]string{"req-000", "req-001", "req-002", "req-003", "req-004"}, customIDs(lines))
	for _, line := range lines {
		require.NotNil(t, line.Response)
		assert.Equal(t, 200, line.Response.StatusCode)
		assert.Equal(t, line.ID, line.Response.RequestID, "the id matches the request the router logged")
		assert.Contains(t, string(line.Response.Body), "answer for "+line.CustomID)
		assert.Nil(t, line.Error)
	}

	record, err := f.store.GetFile(ctx, job.OutputFileID)
	require.NoError(t, err)
	assert.Equal(t, PurposeBatchOutput, record.Purpose)
	assert.Equal(t, "alice", record.Tenant)
}

func TestExecutorRecordsUsage(t *testing.T) {
	f := newExecFixture(t, 3, ExecConfig{})
	ctx := context.Background()
	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	checkpoint, err := f.store.GetCheckpoint(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, Usage{InputTokens: 9, OutputTokens: 15, TotalTokens: 24}, checkpoint.Usage,
		"token usage is summed from the answers")
}

func TestExecutorFailsABadInputFile(t *testing.T) {
	f := newExecFixture(t, 0, ExecConfig{})
	ctx := context.Background()
	_, err := f.files.Create(ctx, "alice", "file-input", strings.NewReader("{oops\n"+batchLine("a")+"\n"), 0)
	require.NoError(t, err)

	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusFailed, job.Status)
	require.Len(t, job.Errors, 1)
	assert.Equal(t, "invalid_json", job.Errors[0].Code)
	assert.Equal(t, int64(1), job.Errors[0].Line)
	assert.Empty(t, job.OutputFileID, "nothing was sent to a model")
	assert.Empty(t, f.dispatcher.dispatched(), "a bad file costs no inference at all")
}

func TestExecutorSendsFailuresToTheErrorFile(t *testing.T) {
	f := newExecFixture(t, 4, ExecConfig{})
	f.dispatcher.respond = func(customID string, call int64) *Response {
		switch customID {
		case "req-001":
			return &Response{StatusCode: 429, Body: []byte(`{"error":"slow down"}`)}
		case "req-002":
			return &Response{Err: errors.New("connection reset")}
		case "req-003":
			return &Response{StatusCode: 200, Body: []byte(`not json at all`)}
		}
		return okResponse(customID)
	}
	ctx := context.Background()
	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, job.Status, "a batch finishes even when requests inside it fail")
	assert.Equal(t, Counts{Total: 4, Completed: 2, Failed: 2}, job.Counts)
	require.NotEmpty(t, job.ErrorFileID)

	failures := f.results(t, job.ErrorFileID)
	require.Len(t, failures, 2)
	byID := map[string]outputLine{}
	for _, line := range failures {
		byID[line.CustomID] = line
	}
	require.Contains(t, byID, "req-001")
	assert.Equal(t, 429, byID["req-001"].Response.StatusCode, "the status code is kept for the user")
	require.Contains(t, byID, "req-002")
	assert.Equal(t, "request_failed", byID["req-002"].Error.Code)

	succeeded := f.results(t, job.OutputFileID)
	assert.ElementsMatch(t, []string{"req-000", "req-003"}, customIDs(succeeded))
	for _, line := range succeeded {
		if line.CustomID == "req-003" {
			assert.JSONEq(t, `"not json at all"`, string(line.Response.Body),
				"a non-JSON answer is kept as a JSON string, so the output file stays valid JSONL")
		}
	}
}

func TestExecutorRefusesAnOversizedAnswer(t *testing.T) {
	f := newExecFixture(t, 2, ExecConfig{MaxResponseBytes: 256})
	f.dispatcher.respond = func(customID string, call int64) *Response {
		if customID == "req-000" {
			return &Response{StatusCode: 200, Body: []byte(`{"text":"` + strings.Repeat("x", 512) + `"}`)}
		}
		return okResponse(customID)
	}
	ctx := context.Background()
	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, Counts{Total: 2, Completed: 1, Failed: 1}, job.Counts)
	failures := f.results(t, job.ErrorFileID)
	require.Len(t, failures, 1)
	assert.Equal(t, "response_too_large", failures[0].Error.Code,
		"one huge answer must not be able to blow the router's memory limit")
}

func TestExecutorResumesAfterTheOwnerDies(t *testing.T) {
	const requests = 12
	f := newExecFixture(t, requests, ExecConfig{Concurrency: 1, CheckpointEvery: 1})
	ctx := context.Background()

	// The first owner answers four requests and is then cut off, as a killed pod would be.
	killed := make(chan struct{})
	var once sync.Once
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	f.dispatcher.onCall = func(call int64) {
		if call == 4 {
			once.Do(func() { close(killed) })
		}
	}
	go func() {
		<-killed
		stopFirst()
	}()

	first := f.claim(t, "router-0")
	err := f.exec.Run(firstCtx, first)
	if err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	require.Equal(t, StatusInProgress, job.Status, "the batch is unfinished")
	checkpoint, err := f.store.GetCheckpoint(ctx, "batch_1")
	require.NoError(t, err)
	done := checkpoint.Counts.Completed + checkpoint.Counts.Failed
	require.Greater(t, done, int64(0), "some work was checkpointed")
	require.Less(t, done, int64(requests), "but not all of it")

	// The lease expires and another replica takes over.
	f.server.fastForward(t, testLeaseTTL+time.Second)
	reaped, err := f.store.Reap(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, reaped, "the reaper hands the orphan back to the queue")

	second := f.claim(t, "router-1")
	require.Greater(t, second.Fence, first.Fence, "the new owner holds a higher fence")
	require.NoError(t, f.exec.Run(ctx, second))

	job, err = f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, job.Status)
	assert.Equal(t, int64(requests), job.Counts.Total)
	assert.Equal(t, int64(requests), job.Counts.Completed+job.Counts.Failed)

	lines := f.results(t, job.OutputFileID)
	lines = append(lines, f.results(t, job.ErrorFileID)...)
	ids := customIDs(lines)
	assert.Len(t, ids, requests, "every request has exactly one answer")
	seen := map[string]int{}
	for _, id := range ids {
		seen[id]++
	}
	for i := 0; i < requests; i++ {
		id := fmt.Sprintf("req-%03d", i)
		assert.Equal(t, 1, seen[id], "%s appears exactly once in the results", id)
	}
	assert.GreaterOrEqual(t, int64(len(f.dispatcher.dispatched())), int64(requests),
		"some requests may be redone, which is the at-least-once trade-off")
}

func TestExecutorStaleOwnerCannotWriteAfterTakeover(t *testing.T) {
	f := newExecFixture(t, 3, ExecConfig{})
	ctx := context.Background()
	stale := f.claim(t, "router-0")

	f.server.fastForward(t, testLeaseTTL+time.Second)
	_, err := f.store.Reap(ctx, 100)
	require.NoError(t, err)
	fresh := f.claim(t, "router-1")
	require.NoError(t, f.exec.Run(ctx, fresh))

	err = f.exec.Run(ctx, stale)
	assert.Error(t, err, "the old owner must not be able to finish the batch a second time")
	assert.True(t, errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrStatusChanged),
		"and the refusal must come from the fence or the status, got: %v", err)

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, job.Status)
	assert.Equal(t, Counts{Total: 3, Completed: 3}, job.Counts, "the counts were not doubled")
}

func TestExecutorCancelKeepsWhatIsAlreadyAnswered(t *testing.T) {
	const requests = 10
	f := newExecFixture(t, requests, ExecConfig{Concurrency: 1, CheckpointEvery: 1})
	ctx := context.Background()

	f.dispatcher.onCall = func(call int64) {
		if call == 3 {
			_, err := f.store.Cancel(ctx, "batch_1")
			assert.NoError(t, err)
		}
	}
	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCancelled, job.Status)
	assert.NotZero(t, job.CancelledAt)
	assert.Equal(t, int64(requests), job.Counts.Completed+job.Counts.Failed,
		"every request is accounted for, answered or not")
	assert.Greater(t, job.Counts.Completed, int64(0), "the answers already paid for are kept")

	answered := f.results(t, job.OutputFileID)
	assert.NotEmpty(t, answered, "partial results are available, as OpenAI promises")

	failures := f.results(t, job.ErrorFileID)
	require.NotEmpty(t, failures)
	assert.Equal(t, "batch_cancelled", failures[0].Error.Code)
	assert.Len(t, append(customIDs(answered), customIDs(failures)...), requests)
}

func TestExecutorExpiredBatchKeepsPartialResults(t *testing.T) {
	f := newExecFixture(t, 4, ExecConfig{Concurrency: 1, CheckpointEvery: 1})
	ctx := context.Background()

	lease := f.claim(t, "router-0")
	f.exec.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	require.NoError(t, f.exec.Run(ctx, lease))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusExpired, job.Status)
	assert.NotZero(t, job.ExpiredAt)
	failures := f.results(t, job.ErrorFileID)
	require.NotEmpty(t, failures)
	assert.Equal(t, "batch_expired", failures[0].Error.Code)
	assert.Equal(t, int64(4), job.Counts.Completed+job.Counts.Failed)
}

func TestExecutorReleasesTheInputFileWhenDone(t *testing.T) {
	f := newExecFixture(t, 2, ExecConfig{})
	ctx := context.Background()
	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	removable, err := f.store.DeleteFile(ctx, "file-input")
	require.NoError(t, err)
	assert.True(t, removable, "once the batch is finished the input file can be deleted")
}

func TestExecutorFailsWhenTheInputFileIsGone(t *testing.T) {
	f := newExecFixture(t, 2, ExecConfig{})
	ctx := context.Background()
	require.NoError(t, f.files.Remove(ctx, "alice", "file-input"))

	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))
	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusFailed, job.Status)
	require.Len(t, job.Errors, 1)
	assert.Equal(t, "input_file_missing", job.Errors[0].Code)
}

func TestExecutorHonoursConcurrency(t *testing.T) {
	f := newExecFixture(t, 20, ExecConfig{Concurrency: 3})
	var inFlight, peak atomic.Int64
	f.dispatcher.respond = func(customID string, call int64) *Response {
		current := inFlight.Add(1)
		for {
			highest := peak.Load()
			if current <= highest || peak.CompareAndSwap(highest, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		inFlight.Add(-1)
		return okResponse(customID)
	}
	require.NoError(t, f.exec.Run(context.Background(), f.claim(t, "router-0")))

	assert.LessOrEqual(t, peak.Load(), int64(3),
		"batch traffic must stay inside its limit so interactive requests keep their share")
	assert.Greater(t, peak.Load(), int64(1), "but it should still work in parallel")
}

func TestExecutorFinalizeIsIdempotent(t *testing.T) {
	f := newExecFixture(t, 6, ExecConfig{Concurrency: 2, CheckpointEvery: 1})
	ctx := context.Background()
	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	first := f.results(t, job.OutputFileID)
	require.Len(t, first, 6)
	callsBefore := f.dispatcher.calls.Load()

	// Put the batch back into finalizing, which is where a pod killed while writing the
	// result files would leave it, and let another replica pick it up.
	require.NoError(t, f.server.client.HSet(ctx, f.store.jobKey("batch_1"),
		"status", string(StatusFinalizing)).Err())
	require.NoError(t, f.server.client.ZAdd(ctx, f.store.queueKey(),
		redisZ(float64(job.CreatedAt), "batch_1")).Err())

	second := f.claim(t, "router-1")
	require.Equal(t, StatusFinalizing, second.Status)
	require.NoError(t, f.exec.Run(ctx, second))

	job, err = f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, job.Status)
	assert.Equal(t, Counts{Total: 6, Completed: 6}, job.Counts, "the counts were not doubled")
	assert.Equal(t, callsBefore, f.dispatcher.calls.Load(), "no request was sent to a model twice")

	again := f.results(t, job.OutputFileID)
	assert.ElementsMatch(t, customIDs(first), customIDs(again), "the output file is the same as before")
	assert.Len(t, again, 6)
}

func TestExecutorNoticesACancelThatArrivesAtTheVeryEnd(t *testing.T) {
	const requests = 3
	// A high checkpoint interval means nothing checks for a cancel during the run, so
	// the only chance to notice one is when the batch is being finished.
	f := newExecFixture(t, requests, ExecConfig{Concurrency: 1, CheckpointEvery: 1000})
	ctx := context.Background()
	f.dispatcher.onCall = func(call int64) {
		if call == requests {
			_, err := f.store.Cancel(ctx, "batch_1")
			assert.NoError(t, err)
		}
	}

	require.NoError(t, f.exec.Run(ctx, f.claim(t, "router-0")))

	job, err := f.store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCancelled, job.Status,
		"the user asked to cancel, so the batch must not report itself as completed")
	assert.NotZero(t, job.CancelledAt)
	assert.Equal(t, int64(requests), job.Counts.Completed, "every answer that was paid for is kept")
	assert.Len(t, f.results(t, job.OutputFileID), requests)
}

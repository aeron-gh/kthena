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
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testLeaseTTL = 2 * time.Second

// testServer runs the suite against miniredis by default, and against the Redis at
// KTHENA_TEST_REDIS_ADDR when that is set.
type testServer struct {
	mini   *miniredis.Miniredis
	client *redis.Client
}

func newTestStore(t *testing.T) (*testServer, *RedisStore) {
	t.Helper()
	if addr := os.Getenv("KTHENA_TEST_REDIS_ADDR"); addr != "" {
		client := redis.NewClient(&redis.Options{Addr: addr})
		require.NoError(t, client.Ping(context.Background()).Err())
		require.NoError(t, client.FlushDB(context.Background()).Err())
		t.Cleanup(func() { _ = client.Close() })
		return &testServer{client: client}, NewRedisStore(client, "")
	}
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return &testServer{mini: mini, client: client}, NewRedisStore(client, "")
}

func (s *testServer) exists(t *testing.T, key string) bool {
	t.Helper()
	n, err := s.client.Exists(context.Background(), key).Result()
	require.NoError(t, err)
	return n == 1
}

func (s *testServer) hget(t *testing.T, key, field string) string {
	t.Helper()
	v, err := s.client.HGet(context.Background(), key, field).Result()
	if errors.Is(err, redis.Nil) {
		return ""
	}
	require.NoError(t, err)
	return v
}

func (s *testServer) fastForward(t *testing.T, d time.Duration) {
	t.Helper()
	if s.mini != nil {
		s.mini.FastForward(d)
		return
	}
	time.Sleep(d)
}

func (s *testServer) freezeTime(at time.Time) bool {
	if s.mini == nil {
		return false
	}
	s.mini.SetTime(at)
	return true
}

func newTestJob(id string, expiresAt int64) *Job {
	return &Job{
		ID:               id,
		Tenant:           "alice",
		Endpoint:         "/v1/chat/completions",
		InputFileID:      "file-" + id,
		CompletionWindow: "24h",
		ExpiresAt:        expiresAt,
		Counts:           Counts{Total: 3},
	}
}

func mustCreate(t *testing.T, store *RedisStore, job *Job) {
	t.Helper()
	require.NoError(t, store.CreateJob(context.Background(), job))
}

func TestCreateJobRoundTrip(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()

	job := newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix())
	job.Metadata = map[string]string{"team": "search"}
	job.Dispatch = Dispatch{
		GatewayKey:   "default/gw1",
		ListenerName: "http",
		ListenerPort: 8080,
		Host:         "api.example.com",
		Headers:      map[string]string{"X-Tier": "gold"},
	}
	mustCreate(t, store, job)

	assert.Equal(t, StatusValidating, job.Status, "status defaults to validating")
	assert.NotZero(t, job.CreatedAt, "created_at is filled in")

	got, err := store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, job.ID, got.ID)
	assert.Equal(t, job.Tenant, got.Tenant)
	assert.Equal(t, StatusValidating, got.Status)
	assert.Equal(t, job.Endpoint, got.Endpoint)
	assert.Equal(t, job.InputFileID, got.InputFileID)
	assert.Equal(t, job.CompletionWindow, got.CompletionWindow)
	assert.Equal(t, job.ExpiresAt, got.ExpiresAt)
	assert.Equal(t, job.CreatedAt, got.CreatedAt)
	assert.Equal(t, job.Counts, got.Counts)
	assert.Equal(t, job.Metadata, got.Metadata)
	assert.Equal(t, job.Dispatch, got.Dispatch)
}

func TestCreateJobRejectsMissingID(t *testing.T) {
	_, store := newTestStore(t)
	assert.Error(t, store.CreateJob(context.Background(), &Job{}))
	assert.Error(t, store.CreateJob(context.Background(), nil))
}

func TestGetJobMissing(t *testing.T) {
	_, store := newTestStore(t)
	_, err := store.GetJob(context.Background(), "batch_nope")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestClaimEmptyQueue(t *testing.T) {
	_, store := newTestStore(t)
	lease, err := store.Claim(context.Background(), "router-0", testLeaseTTL)
	require.NoError(t, err)
	assert.Nil(t, lease, "an empty queue yields no lease")
}

func TestClaimTakesOldestAndHoldsLease(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	future := time.Now().Add(24 * time.Hour).Unix()

	first := newTestJob("batch_first", future)
	first.CreatedAt = 100
	second := newTestJob("batch_second", future)
	second.CreatedAt = 200
	mustCreate(t, store, second)
	mustCreate(t, store, first)

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Equal(t, "batch_first", lease.JobID, "oldest batch is claimed first")
	assert.Equal(t, StatusValidating, lease.Status)
	assert.Equal(t, int64(1), lease.Fence)
	assert.False(t, lease.Overdue)

	assert.Equal(t, "router-0:1", server.hget(t, store.jobKey("batch_first"), "holder")+":"+
		server.hget(t, store.jobKey("batch_first"), "fence"), "holder and fence are recorded on the job")
	assert.True(t, server.exists(t, store.leaseKey("batch_first")), "the lease key is held")

	next, err := store.Claim(ctx, "router-1", testLeaseTTL)
	require.NoError(t, err)
	require.NotNil(t, next)
	assert.Equal(t, "batch_second", next.JobID)
	assert.Equal(t, int64(2), next.Fence, "the fence only moves forward")
}

func TestClaimSkipsFinishedAndExpiresQueuedValidating(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	past := time.Now().Add(-time.Minute).Unix()
	future := time.Now().Add(24 * time.Hour).Unix()

	overdue := newTestJob("batch_overdue", past)
	overdue.CreatedAt = 100
	cancelled := newTestJob("batch_cancelled", future)
	cancelled.CreatedAt = 200
	cancelled.Status = StatusCancelled
	live := newTestJob("batch_live", future)
	live.CreatedAt = 300
	mustCreate(t, store, overdue)
	mustCreate(t, store, cancelled)
	mustCreate(t, store, live)

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Equal(t, "batch_live", lease.JobID, "only the live batch is claimable")

	expired, err := store.GetJob(ctx, "batch_overdue")
	require.NoError(t, err)
	assert.Equal(t, StatusExpired, expired.Status, "a queued validating batch past its deadline expires")
	assert.NotZero(t, expired.ExpiredAt)
	assert.False(t, server.exists(t, store.leaseKey("batch_overdue")), "no lease is taken on it")

	still, err := store.GetJob(ctx, "batch_cancelled")
	require.NoError(t, err)
	assert.Equal(t, StatusCancelled, still.Status, "a finished batch is dropped, not touched")
}

func TestClaimHandsBackOverdueRunningBatch(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()

	job := newTestJob("batch_running", time.Now().Add(-time.Minute).Unix())
	job.Status = StatusInProgress
	mustCreate(t, store, job)

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Equal(t, StatusInProgress, lease.Status)
	assert.True(t, lease.Overdue, "an overdue running batch goes to a worker so its results survive")

	got, err := store.GetJob(ctx, "batch_running")
	require.NoError(t, err)
	assert.Equal(t, StatusInProgress, got.Status, "the script must not expire it behind the worker's back")
}

func TestRenewOnlyByTheOwner(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)

	status, err := store.Renew(ctx, lease, testLeaseTTL)
	require.NoError(t, err)
	assert.Equal(t, StatusValidating, status, "renew reports the current status")

	stranger := &Lease{JobID: lease.JobID, Holder: "router-9", Fence: lease.Fence}
	_, err = store.Renew(ctx, stranger, testLeaseTTL)
	assert.ErrorIs(t, err, ErrLeaseLost)

	_, err = store.Renew(ctx, nil, testLeaseTTL)
	assert.ErrorIs(t, err, ErrLeaseLost)

	server.fastForward(t, testLeaseTTL+time.Second)
	_, err = store.Renew(ctx, lease, testLeaseTTL)
	assert.ErrorIs(t, err, ErrLeaseLost, "an expired lease cannot be renewed")
}

func TestRenewReportsCancelRequest(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NoError(t, store.Transition(ctx, lease, StatusValidating, StatusInProgress, nil))

	status, err := store.Cancel(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCancelling, status)

	status, err = store.Renew(ctx, lease, testLeaseTTL)
	require.NoError(t, err)
	assert.Equal(t, StatusCancelling, status, "the owner learns about the cancel at its next renew")
}

func TestTransitionChecksStatusAndFence(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)

	require.NoError(t, store.Transition(ctx, lease, StatusValidating, StatusInProgress,
		map[string]string{"total": "7"}))

	job, err := store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusInProgress, job.Status)
	assert.NotZero(t, job.InProgressAt, "the timestamp for the new status is written")
	assert.Equal(t, int64(7), job.Counts.Total, "extra fields are written in the same step")

	err = store.Transition(ctx, lease, StatusValidating, StatusFinalizing, nil)
	assert.ErrorIs(t, err, ErrStatusChanged, "a stale expectation is refused")

	stale := &Lease{JobID: lease.JobID, Holder: "router-0", Fence: lease.Fence - 1}
	err = store.Transition(ctx, stale, StatusInProgress, StatusFinalizing, nil)
	assert.ErrorIs(t, err, ErrLeaseLost, "an old owner is refused")

	assert.ErrorIs(t, store.Transition(ctx, nil, StatusInProgress, StatusFinalizing, nil), ErrLeaseLost)
	assert.True(t, server.exists(t, store.leaseKey("batch_1")), "a non-final transition keeps the lease")
}

func TestTransitionToTerminalReleasesOwnership(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NoError(t, store.Transition(ctx, lease, StatusValidating, StatusInProgress, nil))
	require.NoError(t, store.Transition(ctx, lease, StatusInProgress, StatusFinalizing, nil))
	require.NoError(t, store.Transition(ctx, lease, StatusFinalizing, StatusCompleted,
		map[string]string{"output_file_id": "file-out"}))

	job, err := store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, job.Status)
	assert.NotZero(t, job.CompletedAt)
	assert.Equal(t, "file-out", job.OutputFileID)

	assert.False(t, server.exists(t, store.leaseKey("batch_1")), "the lease is released")
	assert.False(t, server.exists(t, store.activeKey()), "the batch leaves the active set")
}

func TestReapRequeuesOrphansAndForgetsFinished(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	future := time.Now().Add(24 * time.Hour).Unix()

	orphan := newTestJob("batch_orphan", future)
	orphan.CreatedAt = 100
	finished := newTestJob("batch_finished", future)
	finished.CreatedAt = 200
	mustCreate(t, store, orphan)
	mustCreate(t, store, finished)

	orphanLease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.Equal(t, "batch_orphan", orphanLease.JobID)
	finishedLease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NoError(t, store.Transition(ctx, finishedLease, StatusValidating, StatusFailed, nil))

	reaped, err := store.Reap(ctx, 500)
	require.NoError(t, err)
	assert.Equal(t, 0, reaped, "a live lease is left alone")

	server.fastForward(t, testLeaseTTL+time.Second)
	reaped, err = store.Reap(ctx, 500)
	require.NoError(t, err)
	assert.Equal(t, 1, reaped, "only the orphan is requeued")

	again, err := store.Claim(ctx, "router-1", testLeaseTTL)
	require.NoError(t, err)
	require.NotNil(t, again)
	assert.Equal(t, "batch_orphan", again.JobID)
	assert.Greater(t, again.Fence, orphanLease.Fence, "the new owner gets a higher fence")

	err = store.Transition(ctx, orphanLease, StatusValidating, StatusInProgress, nil)
	assert.ErrorIs(t, err, ErrLeaseLost, "the dead owner can no longer write")

	count, err := store.Reap(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestCancelQueuedBatch(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))

	status, err := store.Cancel(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCancelled, status, "a queued batch is cancelled outright")

	job, err := store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCancelled, job.Status)
	assert.NotZero(t, job.CancelledAt)

	assert.False(t, server.exists(t, store.queueKey()), "it leaves the queue")

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	assert.Nil(t, lease, "a cancelled batch is never claimed")
}

func TestCancelIsIdempotentAndHonest(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))

	_, err := store.Cancel(ctx, "batch_1")
	require.NoError(t, err)

	status, err := store.Cancel(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusCancelled, status, "cancelling twice reports the same status")

	_, err = store.Cancel(ctx, "batch_missing")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestCancelLeavesFinalizingAlone(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NoError(t, store.Transition(ctx, lease, StatusValidating, StatusInProgress, nil))
	require.NoError(t, store.Transition(ctx, lease, StatusInProgress, StatusFinalizing, nil))

	status, err := store.Cancel(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, StatusFinalizing, status, "the answers are already written, so finish the job")
}

func TestCheckpointRoundTripAndFencing(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)

	_, err = store.GetCheckpoint(ctx, "batch_1")
	assert.ErrorIs(t, err, ErrNotFound)

	checkpoint := &Checkpoint{
		Segments: []Segment{
			{Name: "out-1-ab.jsonl", ValidBytes: 512},
			{Name: "err-1-ab.jsonl", ValidBytes: 64, Errors: true},
		},
		Done:   []byte{0x0f, 0x00},
		Counts: Counts{Total: 10, Completed: 4, Failed: 1},
		Usage:  Usage{InputTokens: 120, OutputTokens: 80, TotalTokens: 200},
	}
	require.NoError(t, store.SaveCheckpoint(ctx, lease, checkpoint))

	got, err := store.GetCheckpoint(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, checkpoint.Segments, got.Segments)
	assert.Equal(t, checkpoint.Done, got.Done)
	assert.Equal(t, checkpoint.Counts, got.Counts)
	assert.Equal(t, checkpoint.Usage, got.Usage)

	stale := &Lease{JobID: lease.JobID, Holder: "router-0", Fence: lease.Fence - 1}
	assert.ErrorIs(t, store.SaveCheckpoint(ctx, stale, checkpoint), ErrLeaseLost,
		"an old owner cannot rewrite progress")
	assert.ErrorIs(t, store.SaveCheckpoint(ctx, nil, checkpoint), ErrLeaseLost)
	assert.Error(t, store.SaveCheckpoint(ctx, lease, nil))
}

func TestTimestampsComeFromRedis(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", 0))

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)

	frozen := time.Unix(1700000000, 0)
	if !server.freezeTime(frozen) {
		t.Skip("clock control needs miniredis")
	}
	require.NoError(t, store.Transition(ctx, lease, StatusValidating, StatusInProgress, nil))

	job, err := store.GetJob(ctx, "batch_1")
	require.NoError(t, err)
	assert.Equal(t, frozen.Unix(), job.InProgressAt, "the clock is Redis's, not the replica's")
}

func TestStatusTerminal(t *testing.T) {
	for _, s := range []Status{StatusCompleted, StatusFailed, StatusExpired, StatusCancelled} {
		assert.True(t, s.Terminal(), string(s))
	}
	for _, s := range []Status{StatusValidating, StatusInProgress, StatusFinalizing, StatusCancelling} {
		assert.False(t, s.Terminal(), string(s))
	}
}

func TestConcurrentClaimsNeverShareABatch(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()
	const jobs, replicas = 20, 8
	future := time.Now().Add(24 * time.Hour).Unix()
	for i := 0; i < jobs; i++ {
		job := newTestJob(fmt.Sprintf("batch_%02d", i), future)
		job.CreatedAt = int64(100 + i)
		mustCreate(t, store, job)
	}

	var mu sync.Mutex
	owners := map[string]string{}
	fences := map[int64]bool{}
	var wg sync.WaitGroup
	for r := 0; r < replicas; r++ {
		wg.Add(1)
		go func(holder string) {
			defer wg.Done()
			for {
				lease, err := store.Claim(ctx, holder, testLeaseTTL)
				require.NoError(t, err)
				if lease == nil {
					return
				}
				mu.Lock()
				if previous, seen := owners[lease.JobID]; seen {
					t.Errorf("%s claimed by %s and %s", lease.JobID, previous, holder)
				}
				owners[lease.JobID] = holder
				if fences[lease.Fence] {
					t.Errorf("fence %d handed out twice", lease.Fence)
				}
				fences[lease.Fence] = true
				mu.Unlock()
			}
		}(fmt.Sprintf("router-%d", r))
	}
	wg.Wait()
	assert.Len(t, owners, jobs, "every batch is claimed exactly once")
	assert.Len(t, fences, jobs, "every claim gets its own fence")
}

func TestScriptsSurviveAScriptFlush(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NotNil(t, lease)

	if err := server.client.ScriptFlush(ctx).Err(); err != nil {
		t.Skipf("SCRIPT FLUSH unsupported: %v", err)
	}
	require.NoError(t, store.Transition(ctx, lease, StatusValidating, StatusInProgress, nil),
		"a flushed script cache must be reloaded, not fatal")

	status, err := store.Renew(ctx, lease, testLeaseTTL)
	require.NoError(t, err)
	assert.Equal(t, StatusInProgress, status)
}

func TestEachStatusWritesItsOwnTimestamp(t *testing.T) {
	cases := []struct {
		status Status
		field  func(*Job) int64
	}{
		{StatusInProgress, func(j *Job) int64 { return j.InProgressAt }},
		{StatusFinalizing, func(j *Job) int64 { return j.FinalizingAt }},
		{StatusCompleted, func(j *Job) int64 { return j.CompletedAt }},
		{StatusFailed, func(j *Job) int64 { return j.FailedAt }},
		{StatusExpired, func(j *Job) int64 { return j.ExpiredAt }},
		{StatusCancelling, func(j *Job) int64 { return j.CancellingAt }},
		{StatusCancelled, func(j *Job) int64 { return j.CancelledAt }},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			_, store := newTestStore(t)
			ctx := context.Background()
			mustCreate(t, store, newTestJob("batch_1", time.Now().Add(24*time.Hour).Unix()))
			lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
			require.NoError(t, err)
			require.NoError(t, store.Transition(ctx, lease, StatusValidating, tc.status, nil))

			job, err := store.GetJob(ctx, "batch_1")
			require.NoError(t, err)
			assert.Equal(t, tc.status, job.Status)
			assert.NotZero(t, tc.field(job), "%s must stamp its own field", tc.status)
			for _, other := range cases {
				if other.status != tc.status {
					assert.Zero(t, other.field(job), "%s must not stamp %s", tc.status, other.status)
				}
			}
		})
	}
}

func TestCorruptRecordsAreReportedNotIgnored(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, store, newTestJob("batch_1", 0))

	require.NoError(t, server.client.HSet(ctx, store.jobKey("batch_1"), "metadata", "{oops").Err())
	_, err := store.GetJob(ctx, "batch_1")
	assert.Error(t, err, "unreadable metadata must surface")

	require.NoError(t, server.client.HSet(ctx, store.jobKey("batch_1"), "metadata", "{}", "dispatch", "{oops").Err())
	_, err = store.GetJob(ctx, "batch_1")
	assert.Error(t, err, "unreadable dispatch must surface")

	lease, err := store.Claim(ctx, "router-0", testLeaseTTL)
	require.NoError(t, err)
	require.NoError(t, store.SaveCheckpoint(ctx, lease, &Checkpoint{}))
	require.NoError(t, server.client.HSet(ctx, store.ckptKey("batch_1"), "segments", "[oops").Err())
	_, err = store.GetCheckpoint(ctx, "batch_1")
	assert.Error(t, err, "unreadable checkpoint segments must surface")
}

func TestFileRecordRoundTrip(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()

	file := &File{
		ID:        "file-1",
		Tenant:    "alice",
		Filename:  "questions.jsonl",
		Purpose:   PurposeBatch,
		Bytes:     2048,
		ExpiresAt: time.Now().Add(720 * time.Hour).Unix(),
	}
	require.NoError(t, store.CreateFile(ctx, file))
	assert.Equal(t, FileUploaded, file.Status, "status defaults to uploaded")
	assert.NotZero(t, file.CreatedAt)

	got, err := store.GetFile(ctx, "file-1")
	require.NoError(t, err)
	assert.Equal(t, file.ID, got.ID)
	assert.Equal(t, file.Tenant, got.Tenant)
	assert.Equal(t, file.Filename, got.Filename)
	assert.Equal(t, file.Purpose, got.Purpose)
	assert.Equal(t, FileUploaded, got.Status)
	assert.Equal(t, file.Bytes, got.Bytes)
	assert.Equal(t, file.CreatedAt, got.CreatedAt)
	assert.Equal(t, file.ExpiresAt, got.ExpiresAt)
	assert.False(t, got.Deleted)

	_, err = store.GetFile(ctx, "file-nope")
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Error(t, store.CreateFile(ctx, &File{}))
	assert.Error(t, store.CreateFile(ctx, nil))
}

func TestDeleteFileWaitsForTheBatchesThatNeedIt(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.CreateFile(ctx, &File{ID: "file-free", Tenant: "alice", Purpose: PurposeBatch}))
	require.NoError(t, store.CreateFile(ctx, &File{ID: "file-busy", Tenant: "alice", Purpose: PurposeBatch}))
	require.NoError(t, server.client.SAdd(ctx, store.fileRefsKey("file-busy"), "batch_1").Err())

	removable, err := store.DeleteFile(ctx, "file-free")
	require.NoError(t, err)
	assert.True(t, removable, "an unused file can go right away")

	removable, err = store.DeleteFile(ctx, "file-busy")
	require.NoError(t, err)
	assert.False(t, removable, "a running batch still needs its input file")

	for _, id := range []string{"file-free", "file-busy"} {
		got, err := store.GetFile(ctx, id)
		require.NoError(t, err)
		assert.True(t, got.Deleted, "%s is marked deleted either way", id)
	}

	_, err = store.DeleteFile(ctx, "file-nope")
	assert.ErrorIs(t, err, ErrNotFound)
}

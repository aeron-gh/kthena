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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// workerFixture runs real workers against one store, the way two router pods would.
type workerFixture struct {
	store      *RedisStore
	files      *FSStore
	dispatcher *fakeDispatcher
	server     *testServer
}

func newWorkerFixture(t *testing.T) *workerFixture {
	t.Helper()
	server, store := newTestStore(t)
	files, _ := newTestFileStore(t)
	return &workerFixture{store: store, files: files, dispatcher: &fakeDispatcher{}, server: server}
}

func (f *workerFixture) addBatch(t *testing.T, id string, requests int) {
	t.Helper()
	ctx := context.Background()
	var content strings.Builder
	for i := 0; i < requests; i++ {
		content.WriteString(batchLine(fmt.Sprintf("%s-req-%03d", id, i)) + "\n")
	}
	fileID := "file-" + id
	_, err := f.files.Create(ctx, "alice", fileID, strings.NewReader(content.String()), 0)
	require.NoError(t, err)
	require.NoError(t, f.store.CreateFile(ctx, &File{
		ID: fileID, Tenant: "alice", Purpose: PurposeBatch, Status: FileProcessed,
	}))
	require.NoError(t, f.store.CreateJob(ctx, &Job{
		ID: id, Tenant: "alice", Endpoint: testEndpoint, InputFileID: fileID,
		CompletionWindow: "24h", ExpiresAt: time.Now().Add(24 * time.Hour).Unix(),
	}))
	require.NoError(t, f.store.HoldFile(ctx, fileID, id))
}

func (f *workerFixture) worker(holder string, config WorkerConfig) *Worker {
	config.Holder = holder
	if config.LeaseTTL == 0 {
		config.LeaseTTL = time.Second
	}
	if config.PollInterval == 0 {
		config.PollInterval = 20 * time.Millisecond
	}
	if config.ReapInterval == 0 {
		config.ReapInterval = 50 * time.Millisecond
	}
	return NewWorker(f.store, f.files, f.dispatcher, config)
}

// waitFor polls until the condition holds, so the tests do not depend on timing.
func waitFor(t *testing.T, timeout time.Duration, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (f *workerFixture) status(t *testing.T, id string) Status {
	t.Helper()
	job, err := f.store.GetJob(context.Background(), id)
	require.NoError(t, err)
	return job.Status
}

func TestWorkerRunsAQueuedBatch(t *testing.T) {
	f := newWorkerFixture(t)
	f.addBatch(t, "batch_1", 4)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var running sync.WaitGroup
	running.Add(1)
	go func() {
		defer running.Done()
		f.worker("router-0", WorkerConfig{}).Run(ctx)
	}()

	waitFor(t, 5*time.Second, "the batch to finish", func() bool {
		return f.status(t, "batch_1") == StatusCompleted
	})
	stop()
	running.Wait()

	job, err := f.store.GetJob(context.Background(), "batch_1")
	require.NoError(t, err)
	assert.Equal(t, Counts{Total: 4, Completed: 4}, job.Counts)
	assert.NotEmpty(t, job.OutputFileID)
}

func TestTwoWorkersNeverRunTheSameBatch(t *testing.T) {
	f := newWorkerFixture(t)
	const batches = 6
	for i := 0; i < batches; i++ {
		f.addBatch(t, fmt.Sprintf("batch_%d", i), 3)
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var running sync.WaitGroup
	for _, holder := range []string{"router-0", "router-1", "router-2"} {
		running.Add(1)
		go func(holder string) {
			defer running.Done()
			f.worker(holder, WorkerConfig{MaxConcurrentJobs: 2}).Run(ctx)
		}(holder)
	}

	waitFor(t, 15*time.Second, "every batch to finish", func() bool {
		for i := 0; i < batches; i++ {
			if f.status(t, fmt.Sprintf("batch_%d", i)) != StatusCompleted {
				return false
			}
		}
		return true
	})
	stop()
	running.Wait()

	seen := map[string]int{}
	for _, id := range f.dispatcher.dispatched() {
		seen[id]++
	}
	for i := 0; i < batches; i++ {
		for r := 0; r < 3; r++ {
			id := fmt.Sprintf("batch_%d-req-%03d", i, r)
			assert.Equal(t, 1, seen[id], "%s must be sent to a model exactly once", id)
		}
	}
}

func TestWorkerHandsBackAnUnfinishedBatchOnShutdown(t *testing.T) {
	f := newWorkerFixture(t)
	f.addBatch(t, "batch_1", 20)

	started := make(chan struct{})
	var once sync.Once
	f.dispatcher.onCall = func(call int64) {
		once.Do(func() { close(started) })
		time.Sleep(20 * time.Millisecond)
	}

	ctx, stop := context.WithCancel(context.Background())
	var running sync.WaitGroup
	running.Add(1)
	go func() {
		defer running.Done()
		f.worker("router-0", WorkerConfig{LeaseTTL: 30 * time.Second,
			Exec: ExecConfig{Concurrency: 1, CheckpointEvery: 1}}).Run(ctx)
	}()

	<-started
	stop()
	running.Wait()

	assert.Equal(t, StatusInProgress, f.status(t, "batch_1"), "the batch is unfinished")
	assert.False(t, f.server.exists(t, f.store.leaseKey("batch_1")),
		"the lease is given up at once instead of blocking the batch for its whole TTL")
	assert.True(t, f.server.exists(t, f.store.queueKey()), "and the batch is back in the queue")

	checkpoint, err := f.store.GetCheckpoint(context.Background(), "batch_1")
	require.NoError(t, err)
	assert.Greater(t, checkpoint.Counts.Completed, int64(0), "the answers already paid for are kept")

	// A second replica picks it up straight away and finishes it.
	next, stopNext := context.WithCancel(context.Background())
	defer stopNext()
	running.Add(1)
	go func() {
		defer running.Done()
		f.worker("router-1", WorkerConfig{}).Run(next)
	}()
	waitFor(t, 20*time.Second, "the other replica to finish the batch", func() bool {
		return f.status(t, "batch_1") == StatusCompleted
	})
	stopNext()
	running.Wait()

	job, err := f.store.GetJob(context.Background(), "batch_1")
	require.NoError(t, err)
	assert.Equal(t, int64(20), job.Counts.Completed+job.Counts.Failed)
}

func TestWorkerKeepsTheLeaseAliveWhileItWorks(t *testing.T) {
	const ttl = 300 * time.Millisecond
	f := newWorkerFixture(t)
	// The work must outlast several lease lifetimes on a real Redis too, where moving
	// the clock forward means actually waiting.
	f.addBatch(t, "batch_1", 40)
	f.dispatcher.onCall = func(call int64) { time.Sleep(40 * time.Millisecond) }

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var running sync.WaitGroup
	running.Add(1)
	go func() {
		defer running.Done()
		f.worker("router-0", WorkerConfig{LeaseTTL: ttl,
			RenewEvery: 50 * time.Millisecond, Exec: ExecConfig{Concurrency: 1}}).Run(ctx)
	}()

	waitFor(t, 10*time.Second, "the batch to start running", func() bool {
		return f.status(t, "batch_1") == StatusInProgress
	})

	// Push the clock past several lease lifetimes while the batch runs. Only the
	// renewals keep the lease alive; miniredis expires keys on this clock, and a real
	// Redis expires them on its own.
	elapsed := time.Duration(0)
	for f.status(t, "batch_1") == StatusInProgress && elapsed < 3*ttl {
		require.True(t, f.server.exists(t, f.store.leaseKey("batch_1")),
			"the lease must stay held for as long as the work takes")
		f.server.fastForward(t, ttl/6)
		elapsed += ttl / 6
		time.Sleep(15 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, elapsed, 2*ttl, "the batch outlived two lease lifetimes")

	waitFor(t, 30*time.Second, "the batch to finish", func() bool {
		return f.status(t, "batch_1") == StatusCompleted
	})
	stop()
	running.Wait()
	assert.Equal(t, int64(40), mustJob(t, f.store, "batch_1").Counts.Completed)
}

func TestWorkerStopsWhenTheLeaseIsStolen(t *testing.T) {
	f := newWorkerFixture(t)
	f.addBatch(t, "batch_1", 30)

	var stolen atomic.Bool
	f.dispatcher.onCall = func(call int64) {
		time.Sleep(10 * time.Millisecond)
		if call == 3 && !stolen.Load() {
			stolen.Store(true)
			// Another replica takes the batch over, as it would after a long GC pause.
			require.NoError(t, f.server.client.Del(context.Background(), f.store.leaseKey("batch_1")).Err())
			_, err := f.store.Reap(context.Background(), 100)
			require.NoError(t, err)
			lease, err := f.store.Claim(context.Background(), "router-thief", 30*time.Second)
			require.NoError(t, err)
			require.NotNil(t, lease)
		}
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.worker("router-0", WorkerConfig{LeaseTTL: 300 * time.Millisecond,
			RenewEvery: 30 * time.Millisecond, MaxConcurrentJobs: 1,
			Exec: ExecConfig{Concurrency: 1, CheckpointEvery: 1}}).Run(ctx)
	}()

	waitFor(t, 10*time.Second, "the first worker to notice the theft", func() bool {
		job, err := f.store.GetJob(context.Background(), "batch_1")
		require.NoError(t, err)
		return job.Holder == "router-thief"
	})
	time.Sleep(300 * time.Millisecond)
	stop()
	<-done

	job := mustJob(t, f.store, "batch_1")
	assert.NotEqual(t, StatusCompleted, job.Status,
		"the replica that lost the lease must not finish the batch behind the new owner's back")
	assert.Equal(t, "router-thief", job.Holder)
}

func TestWorkerReapsOrphansWithoutBeingAsked(t *testing.T) {
	f := newWorkerFixture(t)
	f.addBatch(t, "batch_1", 2)
	ctx := context.Background()

	// A pod claimed this batch and died before writing anything.
	lease, err := f.store.Claim(ctx, "router-dead", 50*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, lease)
	f.server.fastForward(t, 200*time.Millisecond)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.worker("router-0", WorkerConfig{}).Run(runCtx)
	}()

	waitFor(t, 10*time.Second, "the orphan to be picked up and finished", func() bool {
		return f.status(t, "batch_1") == StatusCompleted
	})
	stop()
	<-done
}

func TestWorkerRunsAtMostTheConfiguredJobs(t *testing.T) {
	f := newWorkerFixture(t)
	for i := 0; i < 5; i++ {
		f.addBatch(t, fmt.Sprintf("batch_%d", i), 4)
	}

	var active, peak atomic.Int64
	starts := map[string]bool{}
	var mu sync.Mutex
	f.dispatcher.onCall = func(call int64) { time.Sleep(15 * time.Millisecond) }
	f.dispatcher.respond = func(customID string, call int64) *Response {
		batchID := strings.SplitN(customID, "-req", 2)[0]
		mu.Lock()
		first := !starts[batchID]
		starts[batchID] = true
		mu.Unlock()
		if first {
			current := active.Add(1)
			for {
				highest := peak.Load()
				if current <= highest || peak.CompareAndSwap(highest, current) {
					break
				}
			}
			time.Sleep(40 * time.Millisecond)
			active.Add(-1)
		}
		return okResponse(customID)
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.worker("router-0", WorkerConfig{MaxConcurrentJobs: 2}).Run(ctx)
	}()

	waitFor(t, 30*time.Second, "all batches to finish", func() bool {
		for i := 0; i < 5; i++ {
			if f.status(t, fmt.Sprintf("batch_%d", i)) != StatusCompleted {
				return false
			}
		}
		return true
	})
	stop()
	<-done

	assert.LessOrEqual(t, peak.Load(), int64(2),
		"a replica must not run more batches at once than it was told to")
}

func mustJob(t *testing.T, store *RedisStore, id string) *Job {
	t.Helper()
	job, err := store.GetJob(context.Background(), id)
	require.NoError(t, err)
	return job
}

func TestWorkerStopsSendingRequestsAsSoonAsItLosesTheLease(t *testing.T) {
	const requests = 40
	f := newWorkerFixture(t)
	f.addBatch(t, "batch_1", requests)

	var stolen atomic.Bool
	var callsAtTheft atomic.Int64
	f.dispatcher.onCall = func(call int64) {
		time.Sleep(10 * time.Millisecond)
		if call == 3 && stolen.CompareAndSwap(false, true) {
			ctx := context.Background()
			require.NoError(t, f.server.client.Del(ctx, f.store.leaseKey("batch_1")).Err())
			_, err := f.store.Reap(ctx, 100)
			require.NoError(t, err)
			lease, err := f.store.Claim(ctx, "router-thief", 30*time.Second)
			require.NoError(t, err)
			require.NotNil(t, lease)
			callsAtTheft.Store(call)
		}
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Checkpoints are far apart, so the only thing that can notice the theft
		// quickly is the lease renewal.
		f.worker("router-0", WorkerConfig{LeaseTTL: 300 * time.Millisecond,
			RenewEvery: 25 * time.Millisecond, MaxConcurrentJobs: 1,
			Exec: ExecConfig{Concurrency: 1, CheckpointEvery: 1000}}).Run(ctx)
	}()

	waitFor(t, 10*time.Second, "the theft to happen", func() bool { return stolen.Load() })
	waitFor(t, 10*time.Second, "the worker to give up on the batch", func() bool {
		before := f.dispatcher.calls.Load()
		time.Sleep(150 * time.Millisecond)
		return f.dispatcher.calls.Load() == before
	})
	stop()
	<-done

	sent := f.dispatcher.calls.Load()
	assert.Less(t, sent, int64(requests),
		"a replica that lost the lease must stop spending GPU time on that batch, sent %d of %d", sent, requests)
	assert.Equal(t, "router-thief", mustJob(t, f.store, "batch_1").Holder)
}

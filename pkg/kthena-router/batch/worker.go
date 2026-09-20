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
	"math/rand"
	"os"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

const (
	defaultLeaseTTL     = 30 * time.Second
	defaultPollInterval = 3 * time.Second
	defaultReapInterval = 30 * time.Second
	defaultReapLimit    = 500
	defaultMaxJobs      = 2
	releaseTimeout      = 5 * time.Second
)

// WorkerConfig tunes one replica's share of the batch work.
type WorkerConfig struct {
	Holder            string
	LeaseTTL          time.Duration
	RenewEvery        time.Duration
	PollInterval      time.Duration
	ReapInterval      time.Duration
	ReapLimit         int
	MaxConcurrentJobs int
	Exec              ExecConfig
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.Holder == "" {
		c.Holder = defaultHolder()
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = defaultLeaseTTL
	}
	if c.RenewEvery <= 0 {
		c.RenewEvery = c.LeaseTTL / 3
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = defaultReapInterval
	}
	if c.ReapLimit <= 0 {
		c.ReapLimit = defaultReapLimit
	}
	if c.MaxConcurrentJobs <= 0 {
		c.MaxConcurrentJobs = defaultMaxJobs
	}
	return c
}

// defaultHolder names this replica, preferring the pod name so a stuck lease can be
// traced back to a pod.
func defaultHolder() string {
	if name := os.Getenv("POD_NAME"); name != "" {
		return name
	}
	return "router-" + randomHex(8)
}

// Worker claims batches for this replica, keeps their leases alive while they run, and
// puts unfinished work back when the pod goes away.
type Worker struct {
	store    Store
	executor *Executor
	config   WorkerConfig
}

// NewWorker builds the runner around an executor.
func NewWorker(store Store, files FileStore, dispatcher Dispatcher, config WorkerConfig) *Worker {
	config = config.withDefaults()
	return &Worker{
		store:    store,
		executor: NewExecutor(store, files, dispatcher, config.Exec),
		config:   config,
	}
}

// Holder is the name this replica claims batches under.
func (w *Worker) Holder() string { return w.config.Holder }

// Run works until the context is done, then leaves the batches it was running ready for
// another replica to continue.
func (w *Worker) Run(ctx context.Context) {
	var running sync.WaitGroup
	running.Add(1)
	go func() {
		defer running.Done()
		w.reapLoop(ctx)
	}()
	for i := 0; i < w.config.MaxConcurrentJobs; i++ {
		running.Add(1)
		go func() {
			defer running.Done()
			w.claimLoop(ctx)
		}()
	}
	running.Wait()
	klog.V(2).Infof("batch: worker %s stopped", w.config.Holder)
}

func (w *Worker) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			requeued, err := w.store.Reap(ctx, w.config.ReapLimit)
			if err != nil {
				klog.V(4).Infof("batch: reap failed: %v", err)
				continue
			}
			if requeued > 0 {
				klog.Infof("batch: requeued %d batches whose owner went away", requeued)
			}
		}
	}
}

func (w *Worker) claimLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		lease, err := w.store.Claim(ctx, w.config.Holder, w.config.LeaseTTL)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			klog.V(4).Infof("batch: claim failed: %v", err)
			if !w.wait(ctx) {
				return
			}
			continue
		}
		if lease == nil {
			if !w.wait(ctx) {
				return
			}
			continue
		}
		w.runOne(ctx, lease)
	}
}

// wait sleeps before the next claim, with jitter so replicas do not poll in lockstep.
func (w *Worker) wait(ctx context.Context) bool {
	delay := w.config.PollInterval
	delay += time.Duration(rand.Int63n(int64(delay/2 + 1)))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// runOne runs one batch while renewing its lease, and always leaves the batch in a
// state another replica can continue from.
func (w *Worker) runOne(ctx context.Context, lease *Lease) {
	jobCtx, stop := context.WithCancel(ctx)
	defer stop()

	var renewing sync.WaitGroup
	renewing.Add(1)
	go func() {
		defer renewing.Done()
		w.renewLoop(jobCtx, lease, stop)
	}()

	err := w.executor.Run(jobCtx, lease)
	stop()
	renewing.Wait()

	switch {
	case err == nil:
	case errors.Is(err, ErrLeaseLost), errors.Is(err, ErrStatusChanged):
		klog.V(2).Infof("batch: %s is no longer ours: %v", lease.JobID, err)
	case errors.Is(err, context.Canceled):
		klog.V(2).Infof("batch: %s stopped for shutdown", lease.JobID)
	default:
		klog.Errorf("batch: %s failed: %v", lease.JobID, err)
	}

	// Hand the batch back on a context that is not already cancelled, so a shutdown
	// does not leave it waiting for the lease to time out.
	releaseCtx, done := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer done()
	if err := w.store.Release(releaseCtx, lease); err != nil {
		klog.Errorf("batch: cannot release %s: %v", lease.JobID, err)
	}
}

// renewLoop keeps the lease alive, and stops the run as soon as it is not ours.
func (w *Worker) renewLoop(ctx context.Context, lease *Lease, stop context.CancelFunc) {
	ticker := time.NewTicker(w.config.RenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			status, err := w.store.Renew(ctx, lease, w.config.LeaseTTL)
			if errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrNotFound) {
				klog.Warningf("batch: lost the lease on %s, stopping", lease.JobID)
				stop()
				return
			}
			if err != nil {
				if ctx.Err() == nil {
					klog.V(4).Infof("batch: renew failed for %s: %v", lease.JobID, err)
				}
				continue
			}
			if status.Terminal() {
				return
			}
		}
	}
}

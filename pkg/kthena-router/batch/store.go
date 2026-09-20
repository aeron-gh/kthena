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
	"time"
)

// Store keeps every batch record shared between router replicas, so a batch survives
// the pod that created it.
type Store interface {
	// CreateJob stores a new batch and queues it for a worker.
	CreateJob(ctx context.Context, job *Job) error
	// GetJob returns one batch, or ErrNotFound.
	GetJob(ctx context.Context, id string) (*Job, error)
	// Claim takes ownership of the oldest claimable batch, or returns a nil lease
	// when there is nothing to do.
	Claim(ctx context.Context, holder string, leaseTTL time.Duration) (*Lease, error)
	// Renew extends a lease and reports the batch status, so the owner notices a
	// cancel. It returns ErrLeaseLost once another replica has taken over.
	Renew(ctx context.Context, lease *Lease, leaseTTL time.Duration) (Status, error)
	// Transition moves a batch from one status to the next, but only while this
	// lease still owns it. Terminal statuses also release the lease.
	Transition(ctx context.Context, lease *Lease, from, to Status, fields map[string]string) error
	// Cancel cancels a queued batch outright and asks a running one to stop. It
	// returns the status the batch ended up in.
	Cancel(ctx context.Context, id string) (Status, error)
	// Reap requeues batches whose owner died and forgets finished ones.
	Reap(ctx context.Context, limit int) (int, error)
	// SaveCheckpoint records progress; only the current owner may write one.
	SaveCheckpoint(ctx context.Context, lease *Lease, checkpoint *Checkpoint) error
	// GetCheckpoint returns the progress a new owner should resume from.
	GetCheckpoint(ctx context.Context, id string) (*Checkpoint, error)
	// CreateFile stores the record for an uploaded or produced file.
	CreateFile(ctx context.Context, file *File) error
	// GetFile returns one file record, or ErrNotFound.
	GetFile(ctx context.Context, id string) (*File, error)
	// DeleteFile marks a file deleted and reports whether its bytes can go now,
	// which they cannot while a batch still needs them.
	DeleteFile(ctx context.Context, id string) (bool, error)
}

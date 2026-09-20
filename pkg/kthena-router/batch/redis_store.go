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
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultKeyPrefix keeps every batch key in one Redis slot and away from the other
// router features that share the database.
const DefaultKeyPrefix = "kthena:batch:{b}:"

const defaultClaimScan = 10

//go:embed scripts/*.lua
var scriptFS embed.FS

var (
	claimScript      = mustLoadScript("claim.lua")
	renewScript      = mustLoadScript("renew.lua")
	transitionScript = mustLoadScript("transition.lua")
	reapScript       = mustLoadScript("reap.lua")
	cancelScript     = mustLoadScript("cancel.lua")
	checkpointScript = mustLoadScript("checkpoint.lua")
)

func mustLoadScript(name string) *redis.Script {
	src, err := scriptFS.ReadFile("scripts/" + name)
	if err != nil {
		panic(fmt.Sprintf("batch: cannot read %s: %v", name, err))
	}
	return redis.NewScript(string(src))
}

// RedisStore is the Redis implementation of Store.
type RedisStore struct {
	client    *redis.Client
	prefix    string
	claimScan int
	now       func() time.Time
}

var _ Store = (*RedisStore)(nil)

// NewRedisStore builds a store on an existing Redis client. An empty prefix uses
// DefaultKeyPrefix.
func NewRedisStore(client *redis.Client, prefix string) *RedisStore {
	if prefix == "" {
		prefix = DefaultKeyPrefix
	}
	return &RedisStore{client: client, prefix: prefix, claimScan: defaultClaimScan, now: time.Now}
}

func (s *RedisStore) jobKey(id string) string   { return s.prefix + "job:" + id }
func (s *RedisStore) leaseKey(id string) string { return s.prefix + "lease:" + id }
func (s *RedisStore) ckptKey(id string) string  { return s.prefix + "ckpt:" + id }
func (s *RedisStore) queueKey() string          { return s.prefix + "queue" }
func (s *RedisStore) activeKey() string         { return s.prefix + "active" }
func (s *RedisStore) fenceKey() string          { return s.prefix + "fence" }

func (s *RedisStore) tenantIndexKey(tenant string) string {
	return s.prefix + "idx:batches:" + tenant
}

// CreateJob stores the batch and queues it in one round trip.
func (s *RedisStore) CreateJob(ctx context.Context, job *Job) error {
	if job == nil || job.ID == "" {
		return errors.New("batch: job id is required")
	}
	if job.Status == "" {
		job.Status = StatusValidating
	}
	if job.CreatedAt == 0 {
		job.CreatedAt = s.now().Unix()
	}
	fields, err := job.toHash()
	if err != nil {
		return err
	}
	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, s.jobKey(job.ID), fields...)
	pipe.ZAddNX(ctx, s.queueKey(), redis.Z{Score: float64(job.CreatedAt), Member: job.ID})
	pipe.ZAddNX(ctx, s.tenantIndexKey(job.Tenant), redis.Z{Score: float64(job.CreatedAt), Member: job.ID})
	_, err = pipe.Exec(ctx)
	return err
}

// GetJob returns one batch record.
func (s *RedisStore) GetJob(ctx context.Context, id string) (*Job, error) {
	raw, err := s.client.HGetAll(ctx, s.jobKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNotFound
	}
	return jobFromHash(raw)
}

// Claim takes ownership of the oldest claimable batch.
func (s *RedisStore) Claim(ctx context.Context, holder string, leaseTTL time.Duration) (*Lease, error) {
	keys := []string{s.queueKey(), s.fenceKey(), s.activeKey()}
	res, err := claimScript.Run(ctx, s.client, keys, holder, leaseTTL.Milliseconds(), s.prefix, s.claimScan).Slice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) != 4 {
		return nil, fmt.Errorf("batch: claim returned %d values", len(res))
	}
	fence, err := strconv.ParseInt(fmt.Sprint(res[1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("batch: claim fence %v: %w", res[1], err)
	}
	return &Lease{
		JobID:   fmt.Sprint(res[0]),
		Holder:  holder,
		Fence:   fence,
		Status:  Status(fmt.Sprint(res[2])),
		Overdue: fmt.Sprint(res[3]) == "1",
	}, nil
}

// Renew extends the lease and reports the status the owner should act on.
func (s *RedisStore) Renew(ctx context.Context, lease *Lease, leaseTTL time.Duration) (Status, error) {
	if lease == nil {
		return "", ErrLeaseLost
	}
	keys := []string{s.leaseKey(lease.JobID), s.jobKey(lease.JobID)}
	res, err := renewScript.Run(ctx, s.client, keys, lease.value(), leaseTTL.Milliseconds()).Text()
	if errors.Is(err, redis.Nil) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	if res == "LOST" {
		return "", ErrLeaseLost
	}
	return Status(res), nil
}

// Transition moves the batch to its next status while this lease still owns it.
func (s *RedisStore) Transition(ctx context.Context, lease *Lease, from, to Status, fields map[string]string) error {
	if lease == nil {
		return ErrLeaseLost
	}
	final := "0"
	if to.Terminal() {
		final = "1"
	}
	args := []any{
		string(from),
		strconv.FormatInt(lease.Fence, 10),
		string(to),
		to.timestampField(),
		lease.JobID,
		lease.value(),
		final,
	}
	for k, v := range fields {
		args = append(args, k, v)
	}
	keys := []string{s.jobKey(lease.JobID), s.leaseKey(lease.JobID), s.activeKey()}
	res, err := transitionScript.Run(ctx, s.client, keys, args...).Text()
	if err != nil {
		return err
	}
	switch res {
	case "OK":
		return nil
	case "CONFLICT_FENCE":
		return ErrLeaseLost
	default:
		return ErrStatusChanged
	}
}

// Cancel cancels a queued batch or asks the owner of a running one to stop.
func (s *RedisStore) Cancel(ctx context.Context, id string) (Status, error) {
	keys := []string{s.jobKey(id), s.queueKey(), s.leaseKey(id), s.activeKey()}
	res, err := cancelScript.Run(ctx, s.client, keys, id).Text()
	if err != nil {
		return "", err
	}
	if res == "NOT_FOUND" {
		return "", ErrNotFound
	}
	return Status(res), nil
}

// Reap requeues batches whose owner died.
func (s *RedisStore) Reap(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	keys := []string{s.activeKey(), s.queueKey()}
	return reapScript.Run(ctx, s.client, keys, s.prefix, limit).Int()
}

// SaveCheckpoint records how far the current owner got.
func (s *RedisStore) SaveCheckpoint(ctx context.Context, lease *Lease, checkpoint *Checkpoint) error {
	if lease == nil {
		return ErrLeaseLost
	}
	if checkpoint == nil {
		return errors.New("batch: checkpoint is required")
	}
	segments, err := json.Marshal(checkpoint.Segments)
	if err != nil {
		return err
	}
	args := []any{
		strconv.FormatInt(lease.Fence, 10),
		"segments", string(segments),
		"done", string(checkpoint.Done),
		"total", strconv.FormatInt(checkpoint.Counts.Total, 10),
		"completed", strconv.FormatInt(checkpoint.Counts.Completed, 10),
		"failed", strconv.FormatInt(checkpoint.Counts.Failed, 10),
		"input_tokens", strconv.FormatInt(checkpoint.Usage.InputTokens, 10),
		"output_tokens", strconv.FormatInt(checkpoint.Usage.OutputTokens, 10),
		"total_tokens", strconv.FormatInt(checkpoint.Usage.TotalTokens, 10),
	}
	keys := []string{s.jobKey(lease.JobID), s.ckptKey(lease.JobID)}
	res, err := checkpointScript.Run(ctx, s.client, keys, args...).Text()
	if err != nil {
		return err
	}
	if res != "OK" {
		return ErrLeaseLost
	}
	return nil
}

// GetCheckpoint returns the progress to resume from.
func (s *RedisStore) GetCheckpoint(ctx context.Context, id string) (*Checkpoint, error) {
	raw, err := s.client.HGetAll(ctx, s.ckptKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNotFound
	}
	checkpoint := &Checkpoint{
		Done: []byte(raw["done"]),
		Counts: Counts{
			Total:     parseInt(raw["total"]),
			Completed: parseInt(raw["completed"]),
			Failed:    parseInt(raw["failed"]),
		},
		Usage: Usage{
			InputTokens:  parseInt(raw["input_tokens"]),
			OutputTokens: parseInt(raw["output_tokens"]),
			TotalTokens:  parseInt(raw["total_tokens"]),
		},
	}
	if segments := raw["segments"]; segments != "" {
		if err := json.Unmarshal([]byte(segments), &checkpoint.Segments); err != nil {
			return nil, fmt.Errorf("batch: checkpoint segments: %w", err)
		}
	}
	return checkpoint, nil
}

func parseInt(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func (j *Job) toHash() ([]any, error) {
	metadata, err := json.Marshal(j.Metadata)
	if err != nil {
		return nil, err
	}
	dispatch, err := json.Marshal(j.Dispatch)
	if err != nil {
		return nil, err
	}
	return []any{
		"id", j.ID,
		"tenant", j.Tenant,
		"status", string(j.Status),
		"endpoint", j.Endpoint,
		"input_file_id", j.InputFileID,
		"output_file_id", j.OutputFileID,
		"error_file_id", j.ErrorFileID,
		"completion_window", j.CompletionWindow,
		"metadata", string(metadata),
		"dispatch", string(dispatch),
		"total", j.Counts.Total,
		"completed", j.Counts.Completed,
		"failed", j.Counts.Failed,
		"holder", j.Holder,
		"fence", j.Fence,
		"created_at", j.CreatedAt,
		"expires_at", j.ExpiresAt,
		"in_progress_at", j.InProgressAt,
		"finalizing_at", j.FinalizingAt,
		"completed_at", j.CompletedAt,
		"failed_at", j.FailedAt,
		"expired_at", j.ExpiredAt,
		"cancelling_at", j.CancellingAt,
		"cancelled_at", j.CancelledAt,
	}, nil
}

func jobFromHash(raw map[string]string) (*Job, error) {
	job := &Job{
		ID:               raw["id"],
		Tenant:           raw["tenant"],
		Status:           Status(raw["status"]),
		Endpoint:         raw["endpoint"],
		InputFileID:      raw["input_file_id"],
		OutputFileID:     raw["output_file_id"],
		ErrorFileID:      raw["error_file_id"],
		CompletionWindow: raw["completion_window"],
		Counts: Counts{
			Total:     parseInt(raw["total"]),
			Completed: parseInt(raw["completed"]),
			Failed:    parseInt(raw["failed"]),
		},
		Holder:       raw["holder"],
		Fence:        parseInt(raw["fence"]),
		CreatedAt:    parseInt(raw["created_at"]),
		ExpiresAt:    parseInt(raw["expires_at"]),
		InProgressAt: parseInt(raw["in_progress_at"]),
		FinalizingAt: parseInt(raw["finalizing_at"]),
		CompletedAt:  parseInt(raw["completed_at"]),
		FailedAt:     parseInt(raw["failed_at"]),
		ExpiredAt:    parseInt(raw["expired_at"]),
		CancellingAt: parseInt(raw["cancelling_at"]),
		CancelledAt:  parseInt(raw["cancelled_at"]),
	}
	if metadata := raw["metadata"]; metadata != "" && metadata != "null" {
		if err := json.Unmarshal([]byte(metadata), &job.Metadata); err != nil {
			return nil, fmt.Errorf("batch: job metadata: %w", err)
		}
	}
	if dispatch := raw["dispatch"]; dispatch != "" && dispatch != "null" {
		if err := json.Unmarshal([]byte(dispatch), &job.Dispatch); err != nil {
			return nil, fmt.Errorf("batch: job dispatch: %w", err)
		}
	}
	return job, nil
}

func (s *RedisStore) fileKey(id string) string     { return s.prefix + "file:" + id }
func (s *RedisStore) fileRefsKey(id string) string { return s.prefix + "fileref:" + id }

func (s *RedisStore) fileIndexKey(tenant string) string {
	return s.prefix + "idx:files:" + tenant
}

// CreateFile stores a file record and indexes it for its tenant.
func (s *RedisStore) CreateFile(ctx context.Context, file *File) error {
	if file == nil || file.ID == "" {
		return errors.New("batch: file id is required")
	}
	if file.Status == "" {
		file.Status = FileUploaded
	}
	if file.CreatedAt == 0 {
		file.CreatedAt = s.now().Unix()
	}
	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, s.fileKey(file.ID), file.toHash()...)
	pipe.ZAddNX(ctx, s.fileIndexKey(file.Tenant), redis.Z{Score: float64(file.CreatedAt), Member: file.ID})
	_, err := pipe.Exec(ctx)
	return err
}

// GetFile returns one file record.
func (s *RedisStore) GetFile(ctx context.Context, id string) (*File, error) {
	raw, err := s.client.HGetAll(ctx, s.fileKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNotFound
	}
	return fileFromHash(raw), nil
}

// DeleteFile marks the record deleted, and says whether the bytes can be removed now.
func (s *RedisStore) DeleteFile(ctx context.Context, id string) (bool, error) {
	exists, err := s.client.Exists(ctx, s.fileKey(id)).Result()
	if err != nil {
		return false, err
	}
	if exists == 0 {
		return false, ErrNotFound
	}
	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, s.fileKey(id), "deleted", "1")
	refs := pipe.SCard(ctx, s.fileRefsKey(id))
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return refs.Val() == 0, nil
}

func (f *File) toHash() []any {
	deleted := "0"
	if f.Deleted {
		deleted = "1"
	}
	return []any{
		"id", f.ID,
		"tenant", f.Tenant,
		"filename", f.Filename,
		"purpose", f.Purpose,
		"status", f.Status,
		"bytes", f.Bytes,
		"created_at", f.CreatedAt,
		"expires_at", f.ExpiresAt,
		"deleted", deleted,
	}
}

func fileFromHash(raw map[string]string) *File {
	return &File{
		ID:        raw["id"],
		Tenant:    raw["tenant"],
		Filename:  raw["filename"],
		Purpose:   raw["purpose"],
		Status:    raw["status"],
		Bytes:     parseInt(raw["bytes"]),
		CreatedAt: parseInt(raw["created_at"]),
		ExpiresAt: parseInt(raw["expires_at"]),
		Deleted:   raw["deleted"] == "1",
	}
}

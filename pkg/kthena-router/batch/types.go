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
	"errors"
	"strconv"
)

// Status is the OpenAI batch lifecycle status.
type Status string

const (
	StatusValidating Status = "validating"
	StatusInProgress Status = "in_progress"
	StatusFinalizing Status = "finalizing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusExpired    Status = "expired"
	StatusCancelling Status = "cancelling"
	StatusCancelled  Status = "cancelled"
)

var (
	// ErrNotFound means the batch or checkpoint does not exist.
	ErrNotFound = errors.New("batch: not found")
	// ErrStatusChanged means someone else moved the batch out of the expected status.
	ErrStatusChanged = errors.New("batch: status changed")
	// ErrLeaseLost means another replica owns the batch now.
	ErrLeaseLost = errors.New("batch: lease lost")
)

// Terminal reports whether no further work can happen on a batch in this status.
func (s Status) Terminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusExpired, StatusCancelled:
		return true
	default:
		return false
	}
}

// timestampField is the batch object field that records when this status was entered.
func (s Status) timestampField() string {
	switch s {
	case StatusInProgress:
		return "in_progress_at"
	case StatusFinalizing:
		return "finalizing_at"
	case StatusCompleted:
		return "completed_at"
	case StatusFailed:
		return "failed_at"
	case StatusExpired:
		return "expired_at"
	case StatusCancelling:
		return "cancelling_at"
	case StatusCancelled:
		return "cancelled_at"
	default:
		return ""
	}
}

// File purposes and statuses from the OpenAI files API.
const (
	PurposeBatch       = "batch"
	PurposeBatchOutput = "batch_output"

	FileUploaded  = "uploaded"
	FileProcessed = "processed"
	FileError     = "error"
)

// File is the record for an uploaded input file or a produced result file.
type File struct {
	ID        string
	Tenant    string
	Filename  string
	Purpose   string
	Status    string
	Bytes     int64
	CreatedAt int64
	ExpiresAt int64
	Deleted   bool
}

// Counts is the OpenAI request_counts object.
type Counts struct {
	Total     int64 `json:"total"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
}

// Usage is the token usage aggregated from the responses of a batch.
type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	TotalTokens  int64 `json:"total_tokens"`
}

// Dispatch is the routing context captured when the batch is created and replayed on
// every line, because a batch line carries no listener, host or client headers of its own.
type Dispatch struct {
	GatewayKey   string            `json:"gateway_key,omitempty"`
	ListenerName string            `json:"listener_name,omitempty"`
	ListenerPort int               `json:"listener_port,omitempty"`
	Host         string            `json:"host,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
}

// Job is the stored batch record. It carries the OpenAI batch object plus the fields
// the workers need: the owning tenant, the current holder and its fencing token.
type Job struct {
	ID               string
	Tenant           string
	Status           Status
	Endpoint         string
	InputFileID      string
	OutputFileID     string
	ErrorFileID      string
	CompletionWindow string
	Metadata         map[string]string
	Dispatch         Dispatch
	Counts           Counts
	Holder           string
	Fence            int64
	CreatedAt        int64
	ExpiresAt        int64
	InProgressAt     int64
	FinalizingAt     int64
	CompletedAt      int64
	FailedAt         int64
	ExpiredAt        int64
	CancellingAt     int64
	CancelledAt      int64
}

// Lease is proof that this replica owns a batch until the lease expires.
type Lease struct {
	JobID   string
	Holder  string
	Fence   int64
	Status  Status
	Overdue bool
}

// value is what the lease key holds, so only the current owner can renew or release it.
func (l *Lease) value() string {
	return l.Holder + ":" + strconv.FormatInt(l.Fence, 10)
}

// Segment is one result file written by one ownership attempt. Only the first
// ValidBytes bytes are part of the batch result; anything after them is ignored.
type Segment struct {
	Name       string `json:"name"`
	ValidBytes int64  `json:"valid_bytes"`
	Errors     bool   `json:"errors,omitempty"`
}

// Checkpoint is the progress a worker may resume from after a takeover.
type Checkpoint struct {
	Segments []Segment
	Done     []byte
	Counts   Counts
	Usage    Usage
}

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
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

type createBatchRequest struct {
	InputFileID      string            `json:"input_file_id"`
	Endpoint         string            `json:"endpoint"`
	CompletionWindow string            `json:"completion_window"`
	Metadata         map[string]string `json:"metadata"`
}

func (s *Service) serveBatches(c *gin.Context, method, rest string) {
	switch {
	case rest == "" && method == http.MethodPost:
		s.createBatch(c)
	case rest == "" && method == http.MethodGet:
		s.listBatches(c)
	case strings.HasSuffix(rest, "/cancel") && method == http.MethodPost:
		s.cancelBatch(c, strings.TrimSuffix(rest, "/cancel"))
	case rest != "" && !strings.Contains(rest, "/") && method == http.MethodGet:
		s.getBatch(c, rest)
	default:
		s.fail(c, http.StatusNotFound, "invalid_request_error", "",
			"unknown route "+method+" "+c.Request.URL.Path)
	}
}

func batchObject(job *Job) gin.H {
	out := gin.H{
		"id":                job.ID,
		"object":            "batch",
		"endpoint":          job.Endpoint,
		"input_file_id":     job.InputFileID,
		"completion_window": job.CompletionWindow,
		"status":            string(job.Status),
		"created_at":        job.CreatedAt,
		"request_counts": gin.H{
			"total":     job.Counts.Total,
			"completed": job.Counts.Completed,
			"failed":    job.Counts.Failed,
		},
	}
	for key, value := range map[string]int64{
		"expires_at":     job.ExpiresAt,
		"in_progress_at": job.InProgressAt,
		"finalizing_at":  job.FinalizingAt,
		"completed_at":   job.CompletedAt,
		"failed_at":      job.FailedAt,
		"expired_at":     job.ExpiredAt,
		"cancelling_at":  job.CancellingAt,
		"cancelled_at":   job.CancelledAt,
	} {
		if value != 0 {
			out[key] = value
		}
	}
	if job.OutputFileID != "" {
		out["output_file_id"] = job.OutputFileID
	}
	if job.ErrorFileID != "" {
		out["error_file_id"] = job.ErrorFileID
	}
	if len(job.Metadata) > 0 {
		out["metadata"] = job.Metadata
	}
	if len(job.Errors) > 0 {
		out["errors"] = gin.H{"object": "list", "data": job.Errors}
	}
	return out
}

// dispatchContext records how this request reached the router, because a batch line
// carries no listener, host or client headers of its own. The gateway part is read by
// a function the router wiring supplies, so this package never imports the router.
func (s *Service) dispatchContext(c *gin.Context) Dispatch {
	dispatch := Dispatch{}
	if s.config.GatewayContext != nil {
		dispatch = s.config.GatewayContext(c)
	}
	if dispatch.Host == "" {
		dispatch.Host = c.Request.Host
	}
	for _, name := range s.config.ForwardHeaders {
		if value := c.Request.Header.Get(name); value != "" {
			if dispatch.Headers == nil {
				dispatch.Headers = map[string]string{}
			}
			dispatch.Headers[name] = value
		}
	}
	return dispatch
}

func (s *Service) createBatch(c *gin.Context) {
	tenant := s.tenant(c)
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxCreateBody)
	var request createBatchRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "", "cannot read the request body")
		return
	}
	if request.InputFileID == "" {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "input_file_id", "input_file_id is required")
		return
	}
	if !s.supports(request.Endpoint) {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "endpoint",
			"endpoint "+request.Endpoint+" is not supported for batches")
		return
	}
	if request.CompletionWindow != completionWindow {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "completion_window",
			"completion_window must be "+completionWindow)
		return
	}
	if message := checkMetadata(request.Metadata); message != "" {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "metadata", message)
		return
	}

	file, err := s.store.GetFile(c.Request.Context(), request.InputFileID)
	if err != nil || file.Deleted || file.Tenant != tenant {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "input_file_id",
			"no such file: "+request.InputFileID)
		return
	}
	if file.Purpose != PurposeBatch {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "input_file_id",
			"the input file must have purpose "+PurposeBatch)
		return
	}

	now := s.now()
	job := &Job{
		ID:               s.newID("batch_"),
		Tenant:           tenant,
		Status:           StatusValidating,
		Endpoint:         request.Endpoint,
		InputFileID:      request.InputFileID,
		CompletionWindow: request.CompletionWindow,
		Metadata:         request.Metadata,
		Dispatch:         s.dispatchContext(c),
		CreatedAt:        now.Unix(),
		ExpiresAt:        now.Add(24 * time.Hour).Unix(),
	}
	if err := s.store.HoldFile(c.Request.Context(), file.ID, job.ID); err != nil {
		s.fail(c, http.StatusServiceUnavailable, "api_error", "", "cannot record the batch")
		return
	}
	if err := s.store.CreateJob(c.Request.Context(), job); err != nil {
		if releaseErr := s.store.ReleaseFile(c.Request.Context(), file.ID, job.ID); releaseErr != nil {
			klog.Errorf("batch: cannot release file %s after a failed create: %v", file.ID, releaseErr)
		}
		s.fail(c, http.StatusServiceUnavailable, "api_error", "", "cannot record the batch")
		return
	}
	c.JSON(http.StatusOK, batchObject(job))
}

func checkMetadata(metadata map[string]string) string {
	if len(metadata) > maxMetadata {
		return "metadata may hold at most " + strconv.Itoa(maxMetadata) + " pairs"
	}
	for key, value := range metadata {
		if len(key) > maxMetadataKey {
			return "metadata keys may be at most " + strconv.Itoa(maxMetadataKey) + " characters"
		}
		if len(value) > maxMetadataVal {
			return "metadata values may be at most " + strconv.Itoa(maxMetadataVal) + " characters"
		}
	}
	return ""
}

// batch returns the job only when it belongs to the caller.
func (s *Service) batch(c *gin.Context, id string) (*Job, bool) {
	job, err := s.store.GetJob(c.Request.Context(), id)
	if err != nil || job.Tenant != s.tenant(c) {
		s.fail(c, http.StatusNotFound, "invalid_request_error", "id", "no such batch: "+id)
		return nil, false
	}
	return job, true
}

func (s *Service) getBatch(c *gin.Context, id string) {
	job, ok := s.batch(c, id)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, batchObject(job))
}

func (s *Service) cancelBatch(c *gin.Context, id string) {
	job, ok := s.batch(c, id)
	if !ok {
		return
	}
	if _, err := s.store.Cancel(c.Request.Context(), id); err != nil {
		if errors.Is(err, ErrNotFound) {
			s.fail(c, http.StatusNotFound, "invalid_request_error", "id", "no such batch: "+id)
			return
		}
		s.fail(c, http.StatusServiceUnavailable, "api_error", "", "cannot cancel the batch")
		return
	}
	updated, err := s.store.GetJob(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusOK, batchObject(job))
		return
	}
	c.JSON(http.StatusOK, batchObject(updated))
}

func (s *Service) listBatches(c *gin.Context) {
	limit, ok := s.pageLimit(c)
	if !ok {
		return
	}
	jobs, more, err := s.store.ListJobs(c.Request.Context(), s.tenant(c), c.Query("after"), limit)
	if err != nil {
		s.listFailed(c, err)
		return
	}
	data := make([]gin.H, 0, len(jobs))
	for _, job := range jobs {
		data = append(data, batchObject(job))
	}
	c.JSON(http.StatusOK, listResponse(data, more, func(i int) string { return jobs[i].ID }))
}

func (s *Service) listFiles(c *gin.Context) {
	limit, ok := s.pageLimit(c)
	if !ok {
		return
	}
	files, more, err := s.store.ListFiles(c.Request.Context(), s.tenant(c), c.Query("purpose"), c.Query("after"), limit)
	if err != nil {
		s.listFailed(c, err)
		return
	}
	data := make([]gin.H, 0, len(files))
	for _, file := range files {
		data = append(data, fileObject(file))
	}
	c.JSON(http.StatusOK, listResponse(data, more, func(i int) string { return files[i].ID }))
}

func (s *Service) pageLimit(c *gin.Context) (int, bool) {
	raw := c.Query("limit")
	if raw == "" {
		return defaultPageSize, true
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > maxPageSize {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "limit",
			"limit must be between 1 and "+strconv.Itoa(maxPageSize))
		return 0, false
	}
	return limit, true
}

func (s *Service) listFailed(c *gin.Context, err error) {
	if errors.Is(err, ErrNotFound) {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "after", "unknown cursor")
		return
	}
	s.fail(c, http.StatusServiceUnavailable, "api_error", "", "cannot list right now")
}

func listResponse(data []gin.H, more bool, id func(int) string) gin.H {
	out := gin.H{"object": "list", "data": data, "has_more": more}
	if len(data) > 0 {
		out["first_id"] = id(0)
		out["last_id"] = id(len(data) - 1)
	}
	return out
}

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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

const (
	defaultConcurrency     = 8
	defaultCheckpointEvery = 200
	defaultCheckpointAfter = 5 * time.Second
	defaultMaxResponse     = int64(2 << 20)
	shutdownSaveTimeout    = 10 * time.Second
)

// Request is one line of the input file, ready to send to a model.
type Request struct {
	JobID     string
	Tenant    string
	RequestID string
	Endpoint  string
	Dispatch  Dispatch
	Body      []byte
}

// Response is what came back, or the reason nothing did.
type Response struct {
	StatusCode int
	Body       []byte
	Err        error
}

// Dispatcher sends one batch line through the normal routing path.
type Dispatcher interface {
	Dispatch(ctx context.Context, request *Request) *Response
}

// ExecConfig tunes one worker's execution of a batch.
type ExecConfig struct {
	Limits           Limits
	Concurrency      int
	MaxResponseBytes int64
	CheckpointEvery  int
	CheckpointAfter  time.Duration
}

func (c ExecConfig) withDefaults() ExecConfig {
	c.Limits = c.Limits.withDefaults()
	if c.Concurrency <= 0 {
		c.Concurrency = defaultConcurrency
	}
	if c.MaxResponseBytes <= 0 {
		c.MaxResponseBytes = defaultMaxResponse
	}
	if c.CheckpointEvery <= 0 {
		c.CheckpointEvery = defaultCheckpointEvery
	}
	if c.CheckpointAfter <= 0 {
		c.CheckpointAfter = defaultCheckpointAfter
	}
	return c
}

// Executor runs one claimed batch to its end.
type Executor struct {
	store      Store
	files      FileStore
	dispatcher Dispatcher
	config     ExecConfig
	now        func() time.Time
}

// NewExecutor builds the part of the worker that does the actual work.
func NewExecutor(store Store, files FileStore, dispatcher Dispatcher, config ExecConfig) *Executor {
	return &Executor{store: store, files: files, dispatcher: dispatcher, config: config.withDefaults(), now: time.Now}
}

// outcome says how a run ended, which decides the batch's final status.
type outcome int

const (
	outcomeDone outcome = iota
	outcomeCancelled
	outcomeExpired
)

// Run takes a claimed batch from wherever it is and carries it to a terminal status.
func (e *Executor) Run(ctx context.Context, lease *Lease) error {
	job, err := e.store.GetJob(ctx, lease.JobID)
	if err != nil {
		return err
	}
	if job.Status == StatusValidating {
		ok, err := e.validate(ctx, lease, job)
		if err != nil || !ok {
			return err
		}
		job, err = e.store.GetJob(ctx, lease.JobID)
		if err != nil {
			return err
		}
	}
	if lease.Overdue {
		return e.finish(ctx, lease, job, outcomeExpired, nil)
	}
	if job.Status == StatusCancelling {
		return e.finish(ctx, lease, job, outcomeCancelled, nil)
	}
	return e.execute(ctx, lease, job)
}

// validate reads the input file once. A bad file fails the batch outright, which is
// what OpenAI does, and no inference is ever started for it.
func (e *Executor) validate(ctx context.Context, lease *Lease, job *Job) (bool, error) {
	input, err := e.files.Open(ctx, job.Tenant, job.InputFileID)
	if err != nil {
		return false, e.failJob(ctx, lease, StatusValidating, "input_file_missing", "the input file is gone")
	}
	defer input.Close()

	result, err := ValidateInput(ctx, input, input, job.Endpoint, e.config.Limits)
	if err != nil {
		return false, err
	}
	if !result.OK() {
		failures, err := json.Marshal(result.Errors)
		if err != nil {
			return false, err
		}
		return false, e.store.Transition(ctx, lease, StatusValidating, StatusFailed,
			map[string]string{"errors": string(failures), "total": "0"})
	}
	if _, err := e.files.Create(ctx, job.Tenant, indexID(job.ID), bytes.NewReader(result.Index), 0); err != nil {
		return false, err
	}
	return true, e.store.Transition(ctx, lease, StatusValidating, StatusInProgress,
		map[string]string{"total": fmt.Sprint(result.Total)})
}

func (e *Executor) failJob(ctx context.Context, lease *Lease, from Status, code, message string) error {
	failures, err := json.Marshal([]BatchError{{Code: code, Message: message}})
	if err != nil {
		return err
	}
	return e.store.Transition(ctx, lease, from, StatusFailed, map[string]string{"errors": string(failures)})
}

func indexID(jobID string) string { return jobID + "-index" }

func segmentID(jobID string, fence int64, errors bool) string {
	kind := "out"
	if errors {
		kind = "err"
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		panic("batch: cannot read randomness: " + err.Error())
	}
	return fmt.Sprintf("%s-%s-%d-%s", jobID, kind, fence, hex.EncodeToString(suffix))
}

// progress is the bookkeeping a run carries, and what a checkpoint stores.
type progress struct {
	checkpoint *Checkpoint
	out        *os.File
	errs       *os.File
	outIndex   int
	errIndex   int
	done       []byte
}

func (p *progress) isDone(n int64) bool {
	byteAt := n / 8
	return int(byteAt) < len(p.done) && p.done[byteAt]&(1<<uint(n%8)) != 0
}

func (p *progress) markDone(n int64) {
	byteAt := n / 8
	for int64(len(p.done)) <= byteAt {
		p.done = append(p.done, 0)
	}
	p.done[byteAt] |= 1 << uint(n%8)
}

// execute runs the lines that are not done yet, writing results as they arrive.
func (e *Executor) execute(ctx context.Context, lease *Lease, job *Job) error {
	input, err := e.files.Open(ctx, job.Tenant, job.InputFileID)
	if err != nil {
		return e.failJob(ctx, lease, job.Status, "input_file_missing", "the input file is gone")
	}
	defer input.Close()

	index, err := e.readIndex(ctx, job)
	if err != nil {
		return err
	}
	state, err := e.resume(ctx, lease, job)
	if err != nil {
		return err
	}
	defer state.out.Close()
	defer state.errs.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	pending := make([]int64, 0, len(index))
	for n := int64(0); n < int64(len(index)); n++ {
		if !state.isDone(n) {
			pending = append(pending, n)
		}
	}

	results := make(chan *lineResult, e.config.Concurrency)
	var senders sync.WaitGroup
	slots := make(chan struct{}, e.config.Concurrency)

	go func() {
		defer close(results)
		for _, n := range pending {
			select {
			case slots <- struct{}{}:
			case <-runCtx.Done():
				senders.Wait()
				return
			}
			line, readErr := readLineAt(input, index[n], e.config.Limits.MaxLineBytes)
			senders.Add(1)
			go func(n int64, line []byte, readErr error) {
				defer senders.Done()
				defer func() { <-slots }()
				results <- e.dispatchLine(runCtx, job, n, line, readErr)
			}(n, line, readErr)
		}
		senders.Wait()
	}()

	ending := outcomeDone
	written := 0
	last := e.now()
	for result := range results {
		if err := e.write(state, result); err != nil {
			cancel()
			drain(results)
			return err
		}
		written++
		if written < e.config.CheckpointEvery && e.now().Sub(last) < e.config.CheckpointAfter {
			continue
		}
		if err := e.checkpoint(ctx, lease, state); err != nil {
			cancel()
			drain(results)
			return err
		}
		written, last = 0, e.now()
		if stop := e.shouldStop(ctx, job); stop != outcomeDone {
			ending = stop
			cancel()
			drain(results)
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return e.checkpoint(ctx, lease, state)
	}
	return e.finish(ctx, lease, job, ending, state)
}

func drain(results <-chan *lineResult) {
	for range results {
	}
}

// shouldStop asks Redis whether the user cancelled the batch or its deadline passed.
func (e *Executor) shouldStop(ctx context.Context, job *Job) outcome {
	// Read the record rather than the snapshot this run started with, so both the
	// deadline and a cancel are seen as they are now.
	current, err := e.store.GetJob(ctx, job.ID)
	if err != nil {
		current = job
	}
	if current.Status == StatusCancelling || current.Status == StatusCancelled {
		return outcomeCancelled
	}
	if current.ExpiresAt > 0 && e.now().Unix() > current.ExpiresAt {
		return outcomeExpired
	}
	return outcomeDone
}

func (e *Executor) readIndex(ctx context.Context, job *Job) ([]int64, error) {
	file, err := e.files.Open(ctx, job.Tenant, indexID(job.ID))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	index := make([]int64, 0, len(raw)/indexEntryBytes)
	for at := 0; at+indexEntryBytes <= len(raw); at += indexEntryBytes {
		index = append(index, int64(binary.LittleEndian.Uint64(raw[at:])))
	}
	return index, nil
}

// resume picks up the checkpoint of an earlier owner and starts a fresh pair of
// segments, so nothing the old owner writes from now on can reach the result.
func (e *Executor) resume(ctx context.Context, lease *Lease, job *Job) (*progress, error) {
	checkpoint, err := e.store.GetCheckpoint(ctx, job.ID)
	if errors.Is(err, ErrNotFound) {
		checkpoint = &Checkpoint{}
	} else if err != nil {
		return nil, err
	}
	state := &progress{checkpoint: checkpoint, done: append([]byte(nil), checkpoint.Done...)}
	state.checkpoint.Counts.Total = job.Counts.Total

	outName := segmentID(job.ID, lease.Fence, false)
	errName := segmentID(job.ID, lease.Fence, true)
	state.checkpoint.Segments = append(state.checkpoint.Segments,
		Segment{Name: outName}, Segment{Name: errName, Errors: true})
	state.outIndex = len(state.checkpoint.Segments) - 2
	state.errIndex = len(state.checkpoint.Segments) - 1

	if state.out, err = e.files.Append(ctx, job.Tenant, outName); err != nil {
		return nil, err
	}
	if state.errs, err = e.files.Append(ctx, job.Tenant, errName); err != nil {
		state.out.Close()
		return nil, err
	}
	return state, nil
}

type lineResult struct {
	n         int64
	line      []byte
	requestID string
	response  *Response
	readErr   error
}

func (e *Executor) dispatchLine(ctx context.Context, job *Job, n int64, line []byte, readErr error) *lineResult {
	if readErr != nil {
		return &lineResult{n: n, readErr: readErr}
	}
	var parsed inputLine
	if err := json.Unmarshal(bytes.TrimRight(line, "\r\n"), &parsed); err != nil {
		return &lineResult{n: n, line: line, readErr: err}
	}
	request := &Request{
		JobID:     job.ID,
		Tenant:    job.Tenant,
		RequestID: "batch_req_" + randomHex(12),
		Endpoint:  job.Endpoint,
		Dispatch:  job.Dispatch,
		Body:      bodyOf(line),
	}
	return &lineResult{n: n, line: line, requestID: request.RequestID, response: e.dispatcher.Dispatch(ctx, request)}
}

func randomHex(n int) string {
	buf := make([]byte, n/2)
	if _, err := rand.Read(buf); err != nil {
		panic("batch: cannot read randomness: " + err.Error())
	}
	return hex.EncodeToString(buf)
}

// bodyOf pulls the request body back out of the line without re-encoding it.
func bodyOf(line []byte) []byte {
	var raw struct {
		Body json.RawMessage `json:"body"`
	}
	if json.Unmarshal(bytes.TrimRight(line, "\r\n"), &raw) != nil {
		return nil
	}
	return raw.Body
}

func customIDOf(line []byte) string {
	var parsed inputLine
	if json.Unmarshal(bytes.TrimRight(line, "\r\n"), &parsed) != nil {
		return ""
	}
	return parsed.CustomID
}

type outputLine struct {
	ID       string          `json:"id"`
	CustomID string          `json:"custom_id"`
	Response *outputResponse `json:"response"`
	Error    *outputError    `json:"error"`
}

type outputResponse struct {
	StatusCode int             `json:"status_code"`
	RequestID  string          `json:"request_id"`
	Body       json.RawMessage `json:"body"`
}

type outputError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// write appends one result to the right segment and counts it.
func (e *Executor) write(state *progress, result *lineResult) error {
	customID := customIDOf(result.line)
	entry := outputLine{ID: result.requestID, CustomID: customID}
	if entry.ID == "" {
		entry.ID = "batch_req_" + randomHex(12)
	}
	failed := true

	switch {
	case result.readErr != nil:
		entry.Error = &outputError{Code: "input_unreadable", Message: result.readErr.Error()}
	case result.response != nil && errors.Is(result.response.Err, ErrResponseTooLarge):
		entry.Error = &outputError{Code: "response_too_large",
			Message: fmt.Sprintf("the response is larger than %d bytes", e.config.MaxResponseBytes)}
	case result.response == nil || result.response.Err != nil:
		message := "the request could not be sent"
		if result.response != nil && result.response.Err != nil {
			message = result.response.Err.Error()
		}
		entry.Error = &outputError{Code: "request_failed", Message: message}
	case int64(len(result.response.Body)) > e.config.MaxResponseBytes:
		entry.Error = &outputError{Code: "response_too_large",
			Message: fmt.Sprintf("the response is larger than %d bytes", e.config.MaxResponseBytes)}
	default:
		entry.Response = &outputResponse{
			StatusCode: result.response.StatusCode,
			RequestID:  entry.ID,
			Body:       jsonOrString(result.response.Body),
		}
		failed = result.response.StatusCode < 200 || result.response.StatusCode >= 300
		if !failed {
			e.addUsage(state, result.response.Body)
		}
	}

	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')

	target, at := state.out, state.outIndex
	if failed {
		target, at = state.errs, state.errIndex
	}
	if _, err := target.Write(encoded); err != nil {
		return err
	}
	state.checkpoint.Segments[at].ValidBytes += int64(len(encoded))
	state.markDone(result.n)
	if failed {
		state.checkpoint.Counts.Failed++
	} else {
		state.checkpoint.Counts.Completed++
	}
	return nil
}

// jsonOrString keeps a JSON body as it is, and wraps anything else as a JSON string,
// because the router's own error replies are not always JSON objects.
func jsonOrString(body []byte) json.RawMessage {
	if json.Valid(body) {
		return body
	}
	quoted, err := json.Marshal(string(body))
	if err != nil {
		return json.RawMessage(`""`)
	}
	return quoted
}

func (e *Executor) addUsage(state *progress, body []byte) {
	var parsed struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			TotalTokens      int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &parsed) != nil || parsed.Usage == nil {
		return
	}
	state.checkpoint.Usage.InputTokens += parsed.Usage.PromptTokens
	state.checkpoint.Usage.OutputTokens += parsed.Usage.CompletionTokens
	state.checkpoint.Usage.TotalTokens += parsed.Usage.TotalTokens
}

// checkpoint makes the work so far survive this pod, after the bytes are really on disk.
func (e *Executor) checkpoint(ctx context.Context, lease *Lease, state *progress) error {
	if err := state.out.Sync(); err != nil {
		return err
	}
	if err := state.errs.Sync(); err != nil {
		return err
	}
	state.checkpoint.Done = state.done
	// Saving runs on a context that shutdown cannot cancel: the answers are already
	// paid for, and losing the record of them means paying for them twice.
	saveCtx, done := context.WithTimeout(context.WithoutCancel(ctx), shutdownSaveTimeout)
	defer done()
	return e.store.SaveCheckpoint(saveCtx, lease, state.checkpoint)
}

// finish writes the result files and moves the batch to its terminal status.
func (e *Executor) finish(ctx context.Context, lease *Lease, job *Job, ending outcome, state *progress) error {
	if state != nil {
		if err := e.checkpoint(ctx, lease, state); err != nil {
			return err
		}
	} else {
		checkpoint, err := e.store.GetCheckpoint(ctx, job.ID)
		if errors.Is(err, ErrNotFound) {
			checkpoint = &Checkpoint{}
		} else if err != nil {
			return err
		}
		state = &progress{checkpoint: checkpoint, done: checkpoint.Done}
	}

	// The status may have moved while the lines were running, so read it now rather
	// than trusting the snapshot this run started with.
	for attempt := 0; attempt < 2; attempt++ {
		current, err := e.store.GetJob(ctx, job.ID)
		if err != nil {
			return err
		}
		if current.Status == StatusCancelling {
			ending = outcomeCancelled
		}
		if current.Status == StatusFinalizing {
			break
		}
		err = e.store.Transition(ctx, lease, current.Status, StatusFinalizing, nil)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrStatusChanged) || attempt == 1 {
			return err
		}
	}

	if ending != outcomeDone {
		if err := e.reportUndispatched(ctx, job, state, ending); err != nil {
			return err
		}
	}
	outputID, errorID, err := e.buildResults(ctx, job, state)
	if err != nil {
		return err
	}

	fields := map[string]string{
		"total":     fmt.Sprint(state.checkpoint.Counts.Total),
		"completed": fmt.Sprint(state.checkpoint.Counts.Completed),
		"failed":    fmt.Sprint(state.checkpoint.Counts.Failed),
	}
	if outputID != "" {
		fields["output_file_id"] = outputID
	}
	if errorID != "" {
		fields["error_file_id"] = errorID
	}
	final := StatusCompleted
	switch ending {
	case outcomeCancelled:
		final = StatusCancelled
	case outcomeExpired:
		final = StatusExpired
	}
	if err := e.store.Transition(ctx, lease, StatusFinalizing, final, fields); err != nil {
		return err
	}
	if err := e.store.ReleaseFile(ctx, job.InputFileID, job.ID); err != nil {
		klog.Errorf("batch: cannot release the input file of %s: %v", job.ID, err)
	}
	return nil
}

// reportUndispatched gives every line that never ran an entry in the error file, so a
// cancelled or expired batch still tells the user what happened to each request.
func (e *Executor) reportUndispatched(ctx context.Context, job *Job, state *progress, ending outcome) error {
	code := "batch_cancelled"
	message := "the batch was cancelled before this request ran"
	if ending == outcomeExpired {
		code = "batch_expired"
		message = "the batch expired before this request ran"
	}
	index, err := e.readIndex(ctx, job)
	if err != nil {
		return err
	}
	input, err := e.files.Open(ctx, job.Tenant, job.InputFileID)
	if err != nil {
		return err
	}
	defer input.Close()

	if state.errs == nil {
		name := segmentID(job.ID, time.Now().UnixNano(), true)
		file, err := e.files.Append(ctx, job.Tenant, name)
		if err != nil {
			return err
		}
		defer file.Close()
		state.errs = file
		state.checkpoint.Segments = append(state.checkpoint.Segments, Segment{Name: name, Errors: true})
		state.errIndex = len(state.checkpoint.Segments) - 1
	}

	for n := int64(0); n < int64(len(index)); n++ {
		if state.isDone(n) {
			continue
		}
		line, err := readLineAt(input, index[n], e.config.Limits.MaxLineBytes)
		if err != nil {
			continue
		}
		entry := outputLine{
			ID:       "batch_req_" + randomHex(12),
			CustomID: customIDOf(line),
			Error:    &outputError{Code: code, Message: message},
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		encoded = append(encoded, '\n')
		if _, err := state.errs.Write(encoded); err != nil {
			return err
		}
		state.checkpoint.Segments[state.errIndex].ValidBytes += int64(len(encoded))
		state.markDone(n)
		state.checkpoint.Counts.Failed++
	}
	return state.errs.Sync()
}

// buildResults glues the valid part of every segment into the two result files. Only
// the bytes a checkpoint accounted for are copied, so a stale writer cannot get in.
func (e *Executor) buildResults(ctx context.Context, job *Job, state *progress) (string, string, error) {
	outputID, err := e.buildOne(ctx, job, state, false, job.ID+"-output")
	if err != nil {
		return "", "", err
	}
	errorID, err := e.buildOne(ctx, job, state, true, job.ID+"-errors")
	if err != nil {
		return "", "", err
	}
	return outputID, errorID, nil
}

func (e *Executor) buildOne(ctx context.Context, job *Job, state *progress, errorsFile bool, id string) (string, error) {
	segments := make([]Segment, 0, len(state.checkpoint.Segments))
	for _, segment := range state.checkpoint.Segments {
		if segment.Errors == errorsFile && segment.ValidBytes > 0 {
			segments = append(segments, segment)
		}
	}
	if len(segments) == 0 {
		return "", nil
	}
	readers := make([]io.Reader, 0, len(segments))
	closers := make([]io.Closer, 0, len(segments))
	defer func() {
		for _, closer := range closers {
			closer.Close()
		}
	}()
	for _, segment := range segments {
		file, err := e.files.Open(ctx, job.Tenant, segment.Name)
		if err != nil {
			return "", err
		}
		closers = append(closers, file)
		readers = append(readers, io.LimitReader(file, segment.ValidBytes))
	}

	written, err := e.files.Create(ctx, job.Tenant, id, io.MultiReader(readers...), 0)
	if err != nil {
		return "", err
	}
	record := &File{
		ID:        id,
		Tenant:    job.Tenant,
		Filename:  strings.TrimPrefix(id, job.ID+"-") + ".jsonl",
		Purpose:   PurposeBatchOutput,
		Status:    FileProcessed,
		Bytes:     written,
		CreatedAt: e.now().Unix(),
	}
	if err := e.store.CreateFile(ctx, record); err != nil {
		return "", err
	}
	return id, nil
}

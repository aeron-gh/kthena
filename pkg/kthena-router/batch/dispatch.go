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
	"errors"
	"fmt"
	"net/http"
	"time"

	"k8s.io/klog/v2"
)

// ErrResponseTooLarge means a model answered with more bytes than a batch line may hold.
var ErrResponseTooLarge = errors.New("batch: response is too large")

const defaultLineTimeout = 10 * time.Minute

// Identity is who a batch line belongs to and how its batch reached the router. It
// travels in the request context, never in a header, so no client can forge it.
type Identity struct {
	Tenant   string
	Dispatch Dispatch
}

type identityKey struct{}

// WithIdentity attaches the batch identity to a request context.
func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, identity)
}

// IdentityFromContext returns the batch identity the router wiring should trust.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	return identity, ok
}

// HandlerDispatcher sends each batch line through an in-process HTTP handler, so batch
// traffic takes the same path as a request from a user.
type HandlerDispatcher struct {
	handler     http.Handler
	maxResponse int64
	lineTimeout time.Duration
}

var _ Dispatcher = (*HandlerDispatcher)(nil)

// NewHandlerDispatcher wires the batch worker to the router's own handler.
func NewHandlerDispatcher(handler http.Handler, maxResponse int64, lineTimeout time.Duration) *HandlerDispatcher {
	if maxResponse <= 0 {
		maxResponse = defaultMaxResponse
	}
	if lineTimeout <= 0 {
		lineTimeout = defaultLineTimeout
	}
	return &HandlerDispatcher{handler: handler, maxResponse: maxResponse, lineTimeout: lineTimeout}
}

// Dispatch runs one line and always returns a response, even when the handler panics.
func (d *HandlerDispatcher) Dispatch(ctx context.Context, request *Request) (response *Response) {
	lineCtx, done := context.WithTimeout(ctx, d.lineTimeout)
	defer done()

	defer func() {
		if recovered := recover(); recovered != nil {
			klog.Errorf("batch: request %s panicked: %v", request.RequestID, recovered)
			response = &Response{Err: fmt.Errorf("batch: request panicked: %v", recovered)}
		}
	}()

	identity := Identity{Tenant: request.Tenant, Dispatch: request.Dispatch}
	httpRequest, err := http.NewRequestWithContext(WithIdentity(lineCtx, identity),
		http.MethodPost, request.Endpoint, bytes.NewReader(request.Body))
	if err != nil {
		return &Response{Err: err}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("X-Request-ID", request.RequestID)
	for name, value := range request.Dispatch.Headers {
		httpRequest.Header.Set(name, value)
	}
	if request.Dispatch.Host != "" {
		httpRequest.Host = request.Dispatch.Host
	}
	httpRequest.ContentLength = int64(len(request.Body))
	httpRequest.RemoteAddr = "127.0.0.1:0"

	writer := newBatchWriter(d.maxResponse, lineCtx.Done())
	d.handler.ServeHTTP(writer, httpRequest)

	switch {
	case writer.overflow:
		return &Response{StatusCode: writer.status, Err: ErrResponseTooLarge}
	case lineCtx.Err() != nil && writer.status == 0:
		return &Response{Err: lineCtx.Err()}
	}
	return &Response{StatusCode: writer.statusOrOK(), Body: writer.body.Bytes()}
}

// batchWriter collects one line's answer in memory, up to a limit, and looks enough
// like a real connection for the streaming paths in the router.
type batchWriter struct {
	header   http.Header
	status   int
	body     bytes.Buffer
	limit    int64
	overflow bool
	done     <-chan struct{}
}

func newBatchWriter(limit int64, done <-chan struct{}) *batchWriter {
	return &batchWriter{header: http.Header{}, limit: limit, done: done}
}

func (w *batchWriter) Header() http.Header { return w.header }

func (w *batchWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *batchWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if int64(w.body.Len()+len(data)) > w.limit {
		w.overflow = true
		return 0, ErrResponseTooLarge
	}
	return w.body.Write(data)
}

// Flush is here because the router flushes on streaming paths.
func (w *batchWriter) Flush() {}

// CloseNotify is here because gin's Stream casts the writer to http.CloseNotifier, and
// a writer without it panics. The channel follows this line's context, so the goroutine
// ends when the line does.
func (w *batchWriter) CloseNotify() <-chan bool {
	gone := make(chan bool, 1)
	go func() {
		<-w.done
		gone <- true
	}()
	return gone
}

func (w *batchWriter) statusOrOK() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

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

package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"

	"github.com/volcano-sh/kthena/pkg/kthena-router/batch"
	"github.com/volcano-sh/kthena/pkg/kthena-router/common"
	"github.com/volcano-sh/kthena/pkg/kthena-router/router"
	"github.com/volcano-sh/kthena/pkg/kthena-router/utils"
)

const (
	defaultBatchRoot        = "/var/lib/kthena/batch"
	defaultBatchLineTimeout = 10 * time.Minute
	defaultBatchLeaseTTL    = 30 * time.Second
)

// batchOptions is read from the environment, like DRAIN_TIMEOUT, so the chart stays the
// single place these knobs are set.
type batchOptions struct {
	enabled           bool
	workerEnabled     bool
	root              string
	maxFileBytes      int64
	maxResponseBytes  int64
	maxLineBytes      int64
	maxRequests       int64
	lineTimeout       time.Duration
	leaseTTL          time.Duration
	maxConcurrentJobs int
	concurrency       int
	forwardHeaders    []string
	accessLog         bool
}

func batchOptionsFromEnv() batchOptions {
	options := batchOptions{
		enabled:           envBool("BATCH_ENABLED", false),
		workerEnabled:     envBool("BATCH_WORKER_ENABLED", true),
		root:              envString("BATCH_STORAGE_PATH", defaultBatchRoot),
		maxFileBytes:      envInt64("BATCH_MAX_FILE_BYTES", 200<<20),
		maxResponseBytes:  envInt64("BATCH_MAX_RESPONSE_BYTES", 2<<20),
		maxLineBytes:      envInt64("BATCH_MAX_LINE_BYTES", 1<<20),
		maxRequests:       envInt64("BATCH_MAX_REQUESTS", 50000),
		lineTimeout:       envDuration("BATCH_LINE_TIMEOUT", defaultBatchLineTimeout),
		leaseTTL:          envDuration("BATCH_LEASE_TTL", defaultBatchLeaseTTL),
		maxConcurrentJobs: int(envInt64("BATCH_MAX_CONCURRENT_JOBS", 2)),
		concurrency:       int(envInt64("BATCH_CONCURRENCY", 8)),
		accessLog:         envBool("BATCH_ACCESS_LOG", true),
	}
	for _, name := range strings.Split(os.Getenv("BATCH_FORWARD_HEADERS"), ",") {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			options.forwardHeaders = append(options.forwardHeaders, trimmed)
		}
	}
	return options
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envBool(name string, fallback bool) bool {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		klog.Warningf("Invalid %s %q, using %v", name, value, fallback)
		return fallback
	}
	return parsed
}

func envInt64(name string, fallback int64) int64 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		klog.Warningf("Invalid %s %q, using %d", name, value, fallback)
		return fallback
	}
	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		klog.Warningf("Invalid %s %q, using %v", name, value, fallback)
		return fallback
	}
	return parsed
}

// startBatch turns on the batch API when it is configured, and refuses to start it
// half-working: without Redis or a writable volume the endpoints stay off.
func (s *Server) startBatch(ctx context.Context, gwRouter *router.Router) {
	options := batchOptionsFromEnv()
	if !options.enabled {
		return
	}
	// The volume is checked first: it is a local check, and it fails the same way with
	// or without Redis, which keeps the check itself testable anywhere.
	files := batch.NewFSStore(options.root)
	if err := files.Probe(ctx); err != nil {
		klog.Errorf("Batch API is enabled but %s is not usable: %v; batch stays disabled", options.root, err)
		return
	}
	redisClient := utils.TryGetRedisClient()
	if redisClient == nil {
		klog.Errorf("Batch API is enabled but Redis is unreachable; batch stays disabled")
		return
	}

	store := batch.NewRedisStore(redisClient, "")
	service := batch.NewService(store, files, batch.Config{
		MaxFileBytes:   options.maxFileBytes,
		FileExpiration: 30 * 24 * time.Hour,
		ForwardHeaders: options.forwardHeaders,
		GatewayContext: gatewayContextOf,
	})
	gwRouter.SetAPIExtension(service)
	klog.Infof("Batch API enabled, files under %s", options.root)

	if !options.workerEnabled {
		klog.Info("Batch worker disabled on this replica; it only serves the API")
		return
	}
	dispatcher := batch.NewHandlerDispatcher(
		newBatchEngine(gwRouter, options.accessLog), options.maxResponseBytes, options.lineTimeout)
	worker := batch.NewWorker(store, files, dispatcher, batch.WorkerConfig{
		LeaseTTL:          options.leaseTTL,
		MaxConcurrentJobs: options.maxConcurrentJobs,
		Exec: batch.ExecConfig{
			Limits:           batch.Limits{MaxLineBytes: options.maxLineBytes, MaxRequests: options.maxRequests},
			Concurrency:      options.concurrency,
			MaxResponseBytes: options.maxResponseBytes,
		},
	})
	klog.Infof("Batch worker %s started, %d batches at a time", worker.Holder(), options.maxConcurrentJobs)
	go worker.Run(ctx)
}

// newBatchEngine is the in-process handler chain a batch line travels: recovery, then
// the identity the worker attached, then the same access log and router handler an
// ordinary request would meet. It is never bound to a port.
func newBatchEngine(gwRouter *router.Router, accessLog bool) http.Handler {
	engine := gin.New()
	engine.Use(gin.CustomRecovery(func(c *gin.Context, recovered any) {
		c.AbortWithStatusJSON(http.StatusInternalServerError,
			gin.H{"error": gin.H{"message": fmt.Sprint(recovered), "type": "api_error"}})
	}))
	engine.Use(batchIdentity())
	if accessLog {
		engine.Use(AccessLogMiddleware(gwRouter))
	}
	engine.Any("/*path", gwRouter.HandlerFunc())
	return engine.Handler()
}

// batchIdentity trusts only the identity the worker put in the request context, so a
// header can never decide whose batch a line belongs to.
func batchIdentity() gin.HandlerFunc {
	return func(c *gin.Context) {
		identity, ok := batch.IdentityFromContext(c.Request.Context())
		if !ok {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		if identity.Tenant != "" {
			c.Set(common.UserIdKey, identity.Tenant)
		}
		if identity.Dispatch.GatewayKey != "" {
			c.Set(router.GatewayKey, identity.Dispatch.GatewayKey)
			c.Set(router.GatewayListenerNameKey, identity.Dispatch.ListenerName)
			c.Set(router.GatewayListenerPortKey, identity.Dispatch.ListenerPort)
		}
		c.Next()
	}
}

// gatewayContextOf records which Gateway listener a batch was created through, because
// in Gateway API mode there is no default listener for its lines to fall back on.
func gatewayContextOf(c *gin.Context) batch.Dispatch {
	dispatch := batch.Dispatch{Host: c.Request.Host}
	if key, ok := c.Get(router.GatewayKey); ok {
		dispatch.GatewayKey, _ = key.(string)
	}
	if name, ok := c.Get(router.GatewayListenerNameKey); ok {
		dispatch.ListenerName, _ = name.(string)
	}
	if port, ok := c.Get(router.GatewayListenerPortKey); ok {
		dispatch.ListenerPort, _ = port.(int)
	}
	return dispatch
}

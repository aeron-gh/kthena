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

package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// greedyExtension claims every request, so a missing guard in the router shows up.
type greedyExtension struct {
	served []string
}

func (e *greedyExtension) Handles(method, path string) bool { return true }

func (e *greedyExtension) Serve(c *gin.Context) {
	e.served = append(e.served, c.Request.Method+" "+c.Request.URL.Path)
	c.String(http.StatusOK, "served by the extension")
}

func callRouter(t *testing.T, router *Router, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, path, strings.NewReader(`{"model":"qwen"}`))
	router.HandlerFunc()(c)
	return recorder
}

func TestAPIExtensionServesItsOwnEndpoints(t *testing.T) {
	router, _, backend := setupTestRouter(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer backend.Close()

	require.Nil(t, router.APIExtension(), "there is no extension until one is installed")
	extension := &greedyExtension{}
	router.SetAPIExtension(extension)
	require.Equal(t, extension, router.APIExtension())

	recorder := callRouter(t, router, http.MethodPost, "/v1/batches")
	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "served by the extension", recorder.Body.String())
	assert.Equal(t, []string{"POST /v1/batches"}, extension.served)
}

func TestAPIExtensionOnlyGetsPathsTheAuthMiddlewareCovers(t *testing.T) {
	router, _, backend := setupTestRouter(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer backend.Close()
	extension := &greedyExtension{}
	router.SetAPIExtension(extension)

	// In Gateway API mode the catch-all sits at the root, and the auth middleware only
	// runs for /v1/ paths. Anything outside /v1/ must never reach an extension.
	for _, path := range []string{"/batches", "/files", "/healthz", "/models"} {
		callRouter(t, router, http.MethodPost, path)
	}
	assert.Empty(t, extension.served, "an unauthenticated path must not reach the extension")
}

func TestModelRequestsDoNotGoToTheExtension(t *testing.T) {
	router, _, backend := setupTestRouter(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer backend.Close()

	picky := &pathExtension{prefix: "/v1/batches"}
	router.SetAPIExtension(picky)

	callRouter(t, router, http.MethodPost, "/v1/chat/completions")
	assert.Empty(t, picky.served, "an extension that does not claim a path never sees it")

	callRouter(t, router, http.MethodPost, "/v1/batches")
	assert.Equal(t, []string{"/v1/batches"}, picky.served)
}

// pathExtension claims one prefix, the way the batch API does.
type pathExtension struct {
	prefix string
	served []string
}

func (e *pathExtension) Handles(method, path string) bool {
	return strings.HasPrefix(path, e.prefix)
}

func (e *pathExtension) Serve(c *gin.Context) {
	e.served = append(e.served, c.Request.URL.Path)
	c.Status(http.StatusOK)
}

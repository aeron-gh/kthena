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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/volcano-sh/kthena/pkg/kthena-router/common"
)

const (
	filesPath       = "/v1/files"
	minExpiry       = int64(3600)
	maxExpiry       = int64(2592000)
	maxFormValue    = 1 << 12
	multipartSlack  = 1 << 20
	defaultMaxBytes = int64(200 << 20)
)

// Config holds the operator settings the batch API needs.
type Config struct {
	MaxFileBytes   int64
	FileExpiration time.Duration
}

// Service serves the OpenAI files and batches endpoints from inside the router.
type Service struct {
	store  Store
	files  FileStore
	config Config
	now    func() time.Time
	newID  func(prefix string) string
}

// NewService wires the API onto a metadata store and a file store.
func NewService(store Store, files FileStore, config Config) *Service {
	if config.MaxFileBytes <= 0 {
		config.MaxFileBytes = defaultMaxBytes
	}
	return &Service{store: store, files: files, config: config, now: time.Now, newID: newID}
}

func newID(prefix string) string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		panic("batch: cannot read randomness: " + err.Error())
	}
	return prefix + hex.EncodeToString(buf)
}

// Handles reports whether this request belongs to the batch API.
func (s *Service) Handles(method, path string) bool {
	return path == filesPath || strings.HasPrefix(path, filesPath+"/")
}

// Serve routes one batch API request.
func (s *Service) Serve(c *gin.Context) {
	path := c.Request.URL.Path
	method := c.Request.Method
	rest := strings.Trim(strings.TrimPrefix(path, filesPath), "/")
	switch {
	case rest == "" && method == http.MethodPost:
		s.uploadFile(c)
	case rest != "" && strings.HasSuffix(rest, "/content") && method == http.MethodGet:
		s.fileContent(c, strings.TrimSuffix(rest, "/content"))
	case rest != "" && !strings.Contains(rest, "/") && method == http.MethodGet:
		s.getFile(c, rest)
	case rest != "" && !strings.Contains(rest, "/") && method == http.MethodDelete:
		s.deleteFile(c, rest)
	default:
		s.fail(c, http.StatusNotFound, "invalid_request_error", "", "unknown route "+method+" "+path)
	}
}

func (s *Service) tenant(c *gin.Context) string {
	return c.GetString(common.UserIdKey)
}

// fail answers in the OpenAI error shape, which is not what the rest of the router returns.
func (s *Service) fail(c *gin.Context, status int, kind, param, message string) {
	body := gin.H{"message": message, "type": kind}
	if param != "" {
		body["param"] = param
	}
	c.AbortWithStatusJSON(status, gin.H{"error": body})
}

func (s *Service) notFound(c *gin.Context, id string) {
	s.fail(c, http.StatusNotFound, "invalid_request_error", "id", "no such file: "+id)
}

func fileObject(file *File) gin.H {
	return gin.H{
		"id":         file.ID,
		"object":     "file",
		"bytes":      file.Bytes,
		"created_at": file.CreatedAt,
		"expires_at": file.ExpiresAt,
		"filename":   file.Filename,
		"purpose":    file.Purpose,
		"status":     file.Status,
	}
}

// uploadFile streams the multipart body to the shared volume without buffering it.
func (s *Service) uploadFile(c *gin.Context) {
	tenant := s.tenant(c)
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, s.config.MaxFileBytes+multipartSlack)
	reader, err := c.Request.MultipartReader()
	if err != nil {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "file", "expected a multipart upload")
		return
	}

	id := s.newID("file-")
	var (
		purpose  string
		filename string
		written  int64
		gotFile  bool
		expiry   int64
	)
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.cleanup(c, tenant, id, gotFile)
			s.fail(c, http.StatusBadRequest, "invalid_request_error", "file", "malformed multipart upload")
			return
		}
		switch part.FormName() {
		case "file":
			filename = part.FileName()
			written, err = s.files.Create(c.Request.Context(), tenant, id, part, s.config.MaxFileBytes)
			part.Close()
			if err != nil {
				s.uploadFailed(c, err)
				return
			}
			gotFile = true
		case "purpose":
			purpose = readFormValue(part)
		case "expires_after[seconds]":
			expiry, _ = strconv.ParseInt(readFormValue(part), 10, 64)
		default:
			_, _ = io.Copy(io.Discard, io.LimitReader(part, maxFormValue))
			part.Close()
		}
	}

	if !gotFile {
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "file", "no file part in the upload")
		return
	}
	if purpose != PurposeBatch {
		s.cleanup(c, tenant, id, gotFile)
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "purpose",
			"batch input files must be uploaded with purpose "+PurposeBatch)
		return
	}
	if expiry != 0 && (expiry < minExpiry || expiry > maxExpiry) {
		s.cleanup(c, tenant, id, gotFile)
		s.fail(c, http.StatusBadRequest, "invalid_request_error", "expires_after",
			"expires_after.seconds must be between 3600 and 2592000")
		return
	}

	now := s.now()
	ttl := s.config.FileExpiration
	if expiry != 0 {
		ttl = time.Duration(expiry) * time.Second
	}
	file := &File{
		ID:        id,
		Tenant:    tenant,
		Filename:  filename,
		Purpose:   purpose,
		Status:    FileProcessed,
		Bytes:     written,
		CreatedAt: now.Unix(),
	}
	if ttl > 0 {
		file.ExpiresAt = now.Add(ttl).Unix()
	}
	if err := s.store.CreateFile(c.Request.Context(), file); err != nil {
		s.cleanup(c, tenant, id, gotFile)
		s.fail(c, http.StatusServiceUnavailable, "api_error", "", "cannot record the uploaded file")
		return
	}
	c.JSON(http.StatusOK, fileObject(file))
}

func (s *Service) uploadFailed(c *gin.Context, err error) {
	var tooBig *http.MaxBytesError
	if errors.Is(err, ErrTooLarge) || errors.As(err, &tooBig) {
		s.fail(c, http.StatusRequestEntityTooLarge, "invalid_request_error", "file",
			"file is larger than "+strconv.FormatInt(s.config.MaxFileBytes, 10)+" bytes")
		return
	}
	s.fail(c, http.StatusServiceUnavailable, "api_error", "", "cannot store the uploaded file")
}

func (s *Service) cleanup(c *gin.Context, tenant, id string, wrote bool) {
	if wrote {
		_ = s.files.Remove(c.Request.Context(), tenant, id)
	}
}

func readFormValue(part io.ReadCloser) string {
	defer part.Close()
	value, err := io.ReadAll(io.LimitReader(part, maxFormValue))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(value))
}

// lookup returns the file only when it belongs to the caller and is not deleted.
func (s *Service) lookup(c *gin.Context, id string) (*File, bool) {
	file, err := s.store.GetFile(c.Request.Context(), id)
	if err != nil || file.Deleted || file.Tenant != s.tenant(c) {
		s.notFound(c, id)
		return nil, false
	}
	return file, true
}

func (s *Service) getFile(c *gin.Context, id string) {
	file, ok := s.lookup(c, id)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, fileObject(file))
}

func (s *Service) fileContent(c *gin.Context, id string) {
	file, ok := s.lookup(c, id)
	if !ok {
		return
	}
	body, err := s.files.Open(c.Request.Context(), file.Tenant, file.ID)
	if err != nil {
		s.notFound(c, id)
		return
	}
	defer body.Close()
	c.Header("Content-Type", "application/jsonl")
	http.ServeContent(c.Writer, c.Request, file.Filename, time.Unix(file.CreatedAt, 0), body)
}

func (s *Service) deleteFile(c *gin.Context, id string) {
	file, ok := s.lookup(c, id)
	if !ok {
		return
	}
	removable, err := s.store.DeleteFile(c.Request.Context(), id)
	if err != nil {
		s.notFound(c, id)
		return
	}
	if removable {
		if err := s.files.Remove(c.Request.Context(), file.Tenant, file.ID); err != nil && !errors.Is(err, ErrNotFound) {
			s.fail(c, http.StatusServiceUnavailable, "api_error", "", "cannot delete the file")
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"id": file.ID, "object": "file", "deleted": true})
}

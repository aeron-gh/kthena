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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"strconv"
	"strings"
)

const (
	defaultMaxLineBytes = int64(1 << 20)
	defaultMaxRequests  = int64(50000)
	maxErrors           = 100
	indexEntryBytes     = 8
)

// Limits are the ceilings the validation pass enforces.
type Limits struct {
	MaxLineBytes int64
	MaxRequests  int64
}

func (l Limits) withDefaults() Limits {
	if l.MaxLineBytes <= 0 {
		l.MaxLineBytes = defaultMaxLineBytes
	}
	if l.MaxRequests <= 0 {
		l.MaxRequests = defaultMaxRequests
	}
	return l
}

// Validation is what one pass over an input file found.
type Validation struct {
	Total  int64
	Index  []byte
	Errors []BatchError
}

// OK reports whether the batch may run.
func (v *Validation) OK() bool { return len(v.Errors) == 0 }

// Offset returns where request n starts in the input file.
func (v *Validation) Offset(n int64) int64 {
	return int64(binary.LittleEndian.Uint64(v.Index[n*indexEntryBytes:]))
}

// inputLine parses the body straight into the fields we care about, so a 2 KB prompt
// is never copied into memory just to be checked.
type inputLine struct {
	CustomID string     `json:"custom_id"`
	Method   string     `json:"method"`
	URL      string     `json:"url"`
	Body     *inputBody `json:"body"`
}

type inputBody struct {
	Model  string `json:"model"`
	Stream *bool  `json:"stream"`
}

// hashCustomID is a variable so tests can force collisions onto the recheck path.
var hashCustomID = func(id string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return h.Sum64()
}

// ValidateInput reads the input file once and reports every problem it finds, without
// ever holding more than one line in memory. It also records where each request starts,
// so execution and resume can seek straight to a line.
func ValidateInput(ctx context.Context, src io.Reader, at io.ReaderAt, endpoint string, limits Limits) (*Validation, error) {
	limits = limits.withDefaults()
	result := &Validation{}
	reader := bufio.NewReaderSize(src, 64<<10)
	seen := map[uint64][]int64{}
	index := bytes.Buffer{}

	var offset int64
	buffer := make([]byte, 0, 8<<10)
	for lineNumber := int64(1); ; lineNumber++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		start := offset
		line, consumed, tooLong, err := readLine(reader, buffer[:0], limits.MaxLineBytes)
		buffer = line[:0:cap(line)]
		offset += consumed
		if consumed == 0 && errors.Is(err, io.EOF) {
			break
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if tooLong {
			result.addError("line_too_long", lineNumber, "",
				"line is longer than "+strconv.FormatInt(limits.MaxLineBytes, 10)+" bytes")
			continue
		}
		trimmed := bytes.TrimRight(line, "\r\n")
		if len(bytes.TrimSpace(trimmed)) == 0 {
			continue
		}
		if result.Total >= limits.MaxRequests {
			result.addError("too_many_requests", lineNumber, "",
				"a batch may hold at most "+strconv.FormatInt(limits.MaxRequests, 10)+" requests")
			break
		}
		if !result.checkLine(trimmed, lineNumber, endpoint, at, seen, start, limits) {
			continue
		}
		var entry [indexEntryBytes]byte
		binary.LittleEndian.PutUint64(entry[:], uint64(start))
		index.Write(entry[:])
		result.Total++
	}

	if result.Total == 0 && result.OK() {
		result.addError("empty_file", 0, "", "the input file holds no requests")
	}
	result.Index = index.Bytes()
	return result, nil
}

// checkLine reports whether the line is usable, recording an error when it is not.
func (v *Validation) checkLine(line []byte, lineNumber int64, endpoint string, at io.ReaderAt,
	seen map[uint64][]int64, start int64, limits Limits) bool {
	var parsed inputLine
	if err := json.Unmarshal(line, &parsed); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) && strings.HasPrefix(typeErr.Field, "body") {
			v.addError("invalid_body", lineNumber, "body", "body must be a JSON object")
			return false
		}
		v.addError("invalid_json", lineNumber, "", "line is not a JSON object")
		return false
	}
	if parsed.CustomID == "" {
		v.addError("missing_custom_id", lineNumber, "custom_id", "custom_id is required and must not be empty")
		return false
	}
	if duplicate(at, seen, parsed.CustomID, start, limits.MaxLineBytes) {
		v.addError("duplicate_custom_id", lineNumber, "custom_id",
			"custom_id "+parsed.CustomID+" is used more than once")
		return false
	}
	if parsed.Method != "POST" {
		v.addError("invalid_method", lineNumber, "method", "method must be POST")
		return false
	}
	if parsed.URL != endpoint {
		v.addError("invalid_url", lineNumber, "url", "url must be "+endpoint+" to match the batch endpoint")
		return false
	}
	if parsed.Body == nil {
		v.addError("invalid_body", lineNumber, "body", "body must be a JSON object")
		return false
	}
	body := *parsed.Body
	if body.Model == "" {
		v.addError("missing_model", lineNumber, "body.model", "body.model is required")
		return false
	}
	if body.Stream != nil && *body.Stream {
		v.addError("streaming_unsupported", lineNumber, "body.stream",
			"batch requests cannot stream; remove stream or set it to false")
		return false
	}
	return true
}

// duplicate remembers ids by hash only, and reads the earlier line back to be sure,
// so two different ids that happen to share a hash are not called duplicates.
func duplicate(at io.ReaderAt, seen map[uint64][]int64, id string, start, maxLine int64) bool {
	key := hashCustomID(id)
	for _, offset := range seen[key] {
		earlier, err := readLineAt(at, offset, maxLine)
		if err != nil {
			continue
		}
		var parsed inputLine
		if json.Unmarshal(bytes.TrimRight(earlier, "\r\n"), &parsed) == nil && parsed.CustomID == id {
			return true
		}
	}
	seen[key] = append(seen[key], start)
	return false
}

func (v *Validation) addError(code string, line int64, param, message string) {
	if len(v.Errors) >= maxErrors {
		return
	}
	v.Errors = append(v.Errors, BatchError{Code: code, Line: line, Message: message, Param: param})
}

// readLine fills the caller's buffer with one line, so a whole file does not churn
// through the allocator. It reports how many bytes the line occupied and whether it was
// longer than the limit, in which case it is skipped without being held in memory.
func readLine(reader *bufio.Reader, line []byte, max int64) ([]byte, int64, bool, error) {
	var (
		consumed int64
		tooLong  bool
	)
	for {
		chunk, err := reader.ReadSlice('\n')
		consumed += int64(len(chunk))
		if !tooLong {
			if int64(len(line))+int64(len(chunk)) > max {
				tooLong = true
				line = line[:0]
			} else {
				line = append(line, chunk...)
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, consumed, tooLong, err
	}
}

func readLineAt(at io.ReaderAt, offset, max int64) ([]byte, error) {
	buffer := make([]byte, min64(max, 64<<10))
	n, err := at.ReadAt(buffer, offset)
	if n == 0 && err != nil {
		return nil, err
	}
	buffer = buffer[:n]
	if cut := bytes.IndexByte(buffer, '\n'); cut >= 0 {
		return buffer[:cut], nil
	}
	return buffer, nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

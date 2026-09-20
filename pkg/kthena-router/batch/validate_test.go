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
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testEndpoint = "/v1/chat/completions"

func validLine(id string) string {
	return `{"custom_id":"` + id + `","method":"POST","url":"` + testEndpoint +
		`","body":{"model":"qwen","messages":[{"role":"user","content":"hi"}]}}`
}

func validate(t *testing.T, content string, limits Limits) *Validation {
	t.Helper()
	reader := strings.NewReader(content)
	result, err := ValidateInput(context.Background(), reader, strings.NewReader(content), testEndpoint, limits)
	require.NoError(t, err)
	return result
}

func TestValidateAcceptsAGoodFile(t *testing.T) {
	content := validLine("a") + "\n" + validLine("b") + "\n" + validLine("c") + "\n"
	result := validate(t, content, Limits{})

	assert.True(t, result.OK(), "errors: %v", result.Errors)
	assert.Equal(t, int64(3), result.Total)
	require.Len(t, result.Index, 3*indexEntryBytes)

	for n, want := range []string{"a", "b", "c"} {
		offset := result.Offset(int64(n))
		line := content[offset:]
		if cut := strings.IndexByte(line, '\n'); cut >= 0 {
			line = line[:cut]
		}
		var parsed inputLine
		require.NoError(t, json.Unmarshal([]byte(line), &parsed))
		assert.Equal(t, want, parsed.CustomID, "the index points at request %d", n)
	}
}

func TestValidateHandlesBlankLinesAndCRLF(t *testing.T) {
	content := validLine("a") + "\r\n\r\n" + validLine("b") + "\n   \n" + validLine("c")
	result := validate(t, content, Limits{})

	assert.True(t, result.OK(), "errors: %v", result.Errors)
	assert.Equal(t, int64(3), result.Total, "blank lines are skipped and a missing final newline is fine")
}

func TestValidateReportsEveryKindOfBadLine(t *testing.T) {
	cases := []struct {
		name string
		line string
		code string
	}{
		{"not json", `{oops`, "invalid_json"},
		{"not an object", `[1,2,3]`, "invalid_json"},
		{"no custom_id", `{"method":"POST","url":"` + testEndpoint + `","body":{"model":"m"}}`, "missing_custom_id"},
		{"empty custom_id", `{"custom_id":"","method":"POST","url":"` + testEndpoint + `","body":{"model":"m"}}`, "missing_custom_id"},
		{"wrong method", `{"custom_id":"a","method":"GET","url":"` + testEndpoint + `","body":{"model":"m"}}`, "invalid_method"},
		{"wrong url", `{"custom_id":"a","method":"POST","url":"/v1/embeddings","body":{"model":"m"}}`, "invalid_url"},
		{"no body", `{"custom_id":"a","method":"POST","url":"` + testEndpoint + `"}`, "invalid_body"},
		{"body not an object", `{"custom_id":"a","method":"POST","url":"` + testEndpoint + `","body":"hi"}`, "invalid_body"},
		{"no model", `{"custom_id":"a","method":"POST","url":"` + testEndpoint + `","body":{"messages":[]}}`, "missing_model"},
		{"streaming", `{"custom_id":"a","method":"POST","url":"` + testEndpoint + `","body":{"model":"m","stream":true}}`, "streaming_unsupported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := validate(t, tc.line+"\n", Limits{})
			require.Len(t, result.Errors, 1, "errors: %v", result.Errors)
			assert.Equal(t, tc.code, result.Errors[0].Code)
			assert.Equal(t, int64(1), result.Errors[0].Line, "the user needs the line number")
			assert.Equal(t, int64(0), result.Total, "a bad line is not counted as a request")
		})
	}
}

func TestValidateAllowsStreamFalse(t *testing.T) {
	line := `{"custom_id":"a","method":"POST","url":"` + testEndpoint + `","body":{"model":"m","stream":false}}`
	result := validate(t, line+"\n", Limits{})
	assert.True(t, result.OK(), "stream:false is fine, only stream:true is refused: %v", result.Errors)
}

func TestValidateFindsDuplicateCustomIDs(t *testing.T) {
	content := validLine("a") + "\n" + validLine("b") + "\n" + validLine("a") + "\n"
	result := validate(t, content, Limits{})

	require.Len(t, result.Errors, 1)
	assert.Equal(t, "duplicate_custom_id", result.Errors[0].Code)
	assert.Equal(t, int64(3), result.Errors[0].Line, "the second use is the one reported")
	assert.Contains(t, result.Errors[0].Message, "a")
	assert.Equal(t, int64(2), result.Total, "the first two requests still count")
}

func TestValidateDoesNotCallAHashCollisionADuplicate(t *testing.T) {
	original := hashCustomID
	hashCustomID = func(string) uint64 { return 42 }
	t.Cleanup(func() { hashCustomID = original })

	content := validLine("a") + "\n" + validLine("b") + "\n" + validLine("c") + "\n"
	result := validate(t, content, Limits{})
	assert.True(t, result.OK(), "every id hashes the same here, but they are all different: %v", result.Errors)
	assert.Equal(t, int64(3), result.Total)

	withDuplicate := content + validLine("b") + "\n"
	result = validate(t, withDuplicate, Limits{})
	require.Len(t, result.Errors, 1, "a real duplicate is still found when everything collides")
	assert.Equal(t, "duplicate_custom_id", result.Errors[0].Code)
	assert.Equal(t, int64(4), result.Errors[0].Line)
}

func TestValidateSkipsAnOverlongLineAndKeepsGoing(t *testing.T) {
	huge := `{"custom_id":"big","method":"POST","url":"` + testEndpoint +
		`","body":{"model":"m","prompt":"` + strings.Repeat("x", 4096) + `"}}`
	content := validLine("a") + "\n" + huge + "\n" + validLine("b") + "\n"

	result := validate(t, content, Limits{MaxLineBytes: 512})
	require.Len(t, result.Errors, 1)
	assert.Equal(t, "line_too_long", result.Errors[0].Code)
	assert.Equal(t, int64(2), result.Errors[0].Line)
	assert.Equal(t, int64(2), result.Total, "the lines around it are still accepted")

	for n, want := range []string{"a", "b"} {
		offset := result.Offset(int64(n))
		var parsed inputLine
		line := content[offset:]
		require.NoError(t, json.Unmarshal([]byte(line[:strings.IndexByte(line, '\n')]), &parsed))
		assert.Equal(t, want, parsed.CustomID, "offsets stay correct across the skipped line")
	}
}

func TestValidateStopsAtTheRequestLimit(t *testing.T) {
	var content strings.Builder
	for i := 0; i < 5; i++ {
		content.WriteString(validLine(fmt.Sprintf("id-%d", i)) + "\n")
	}
	result := validate(t, content.String(), Limits{MaxRequests: 3})

	assert.Equal(t, int64(3), result.Total)
	require.Len(t, result.Errors, 1)
	assert.Equal(t, "too_many_requests", result.Errors[0].Code)
}

func TestValidateRejectsAnEmptyFile(t *testing.T) {
	for _, content := range []string{"", "\n", "   \n\n"} {
		result := validate(t, content, Limits{})
		require.Len(t, result.Errors, 1, "content %q", content)
		assert.Equal(t, "empty_file", result.Errors[0].Code)
		assert.Equal(t, int64(0), result.Total)
	}
}

func TestValidateCapsTheErrorList(t *testing.T) {
	var content strings.Builder
	for i := 0; i < maxErrors+50; i++ {
		content.WriteString("{oops\n")
	}
	result := validate(t, content.String(), Limits{})
	assert.Len(t, result.Errors, maxErrors, "a broken file must not fill Redis with errors")
}

func TestValidateStopsWhenTheContextIsCancelled(t *testing.T) {
	var content strings.Builder
	for i := 0; i < 1000; i++ {
		content.WriteString(validLine(fmt.Sprintf("id-%d", i)) + "\n")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	reader := strings.NewReader(content.String())
	_, err := ValidateInput(ctx, reader, strings.NewReader(content.String()), testEndpoint, Limits{})
	assert.ErrorIs(t, err, context.Canceled, "shutdown must not wait for a 200MB file")
}

func TestValidateHoldsOnlyOneLineInMemory(t *testing.T) {
	build := func(lines int, padding int) string {
		var content strings.Builder
		pad := strings.Repeat("x", padding)
		for i := 0; i < lines; i++ {
			content.WriteString(`{"custom_id":"id-` + fmt.Sprintf("%06d", i) +
				`","method":"POST","url":"` + testEndpoint +
				`","body":{"model":"qwen","prompt":"` + pad + `"}}` + "\n")
		}
		return content.String()
	}
	measure := func(body string) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		result := validate(t, body, Limits{})
		runtime.ReadMemStats(&after)
		require.True(t, result.OK(), "errors: %v", result.Errors)
		return after.TotalAlloc - before.TotalAlloc
	}

	const lines = 20000
	small := build(lines, 8)
	large := build(lines, 2048)
	require.Greater(t, len(large), 8*len(small), "the second file is far bigger, with the same number of requests")

	smallCost := measure(small)
	largeCost := measure(large)

	assert.Less(t, largeCost, 2*smallCost,
		"a file %d times bigger cost %d instead of %d; validation must not hold the file",
		len(large)/len(small), largeCost, smallCost)
	assert.Less(t, smallCost/uint64(lines), uint64(1024),
		"validation should cost well under a kilobyte of allocation per request")
}

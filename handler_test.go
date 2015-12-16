package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/DeviantArt/centrifugo-scriber/Godeps/_workspace/src/github.com/quipo/statsd"
	scribe "github.com/DeviantArt/centrifugo-scriber/gen-go/scribe"
)

func TestParsingScribeMessages(t *testing.T) {
	type testCase struct {
		name      string
		input     []*scribe.LogEntry
		expectOut *centrifugoRedisRequest
		expectErr bool
	}

	now := time.Now()
	tests := []testCase{
		{
			name:  "Empty input",
			input: []*scribe.LogEntry{},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{},
			},
			expectErr: false,
		},
		{
			name: "Single valid event input",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message:  "{\"channel\":\"foo\", \"data\":{\"foo\": 1234}}",
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "publish",
						Params: centrifugoPublishParams{
							Channel: "foo",
							Data:    json.RawMessage("{\"foo\": 1234}"),
						},
					},
				},
			},
			expectErr: false,
		},
		{
			name: "Single valid event with OK TTL",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo\", \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "publish",
						Params: centrifugoPublishParams{
							Channel: "foo",
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}",
								now.Unix())),
						},
					},
				},
			},
			expectErr: false,
		},
		{
			name: "Single valid event with expired TTL",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo\", \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Add(-1*time.Hour).Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{},
			},
			expectErr: false,
		},
		{
			name: "Multiple events with OK TTL",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo\", \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Add(-30*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo2\", \"data\":{\"ts\": %d, \"ttl\":30, \"data\":{\"foo\": 5678}}}",
						now.Add(-20*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo3\", \"data\":{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}}",
						now.Add(-2*time.Second).Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "publish",
						Params: centrifugoPublishParams{
							Channel: "foo",
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}",
								now.Add(-30*time.Second).Unix())),
						},
					},
					{
						Method: "publish",
						Params: centrifugoPublishParams{
							Channel: "foo2",
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":30, \"data\":{\"foo\": 5678}}",
								now.Add(-20*time.Second).Unix())),
						},
					},
					{
						Method: "publish",
						Params: centrifugoPublishParams{
							Channel: "foo3",
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}",
								now.Add(-2*time.Second).Unix())),
						},
					},
				},
			},
			expectErr: false,
		},
		{
			name: "Multiple events with mixed TTL",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo\", \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Add(-30*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo2\", \"data\":{\"ts\": %d, \"ttl\":10, \"data\":{\"foo\": 5678}}}",
						now.Add(-20*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo3\", \"data\":{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}}",
						now.Add(-2*time.Second).Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "publish",
						Params: centrifugoPublishParams{
							Channel: "foo",
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}",
								now.Add(-30*time.Second).Unix())),
						},
					},
					// event 2 expired
					{
						Method: "publish",
						Params: centrifugoPublishParams{
							Channel: "foo3",
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}",
								now.Add(-2*time.Second).Unix())),
						},
					},
				},
			},
			expectErr: false,
		},
		{
			name: "Multiple events with mixed validity",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo\", \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Add(-30*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo2\"}", // invalid no data field
						now.Add(-20*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channel\":\"foo3\", \"data\":{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}}",
						now.Add(-2*time.Second).Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "publish",
						Params: centrifugoPublishParams{
							Channel: "foo",
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}",
								now.Add(-30*time.Second).Unix())),
						},
					},
					// event 2 is invalid
					{
						Method: "publish",
						Params: centrifugoPublishParams{
							Channel: "foo3",
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}",
								now.Add(-2*time.Second).Unix())),
						},
					},
				},
			},
			expectErr: false,
		},
	}

	for _, test := range tests {
		out, err := scribeEntriesToPublishCommand(test.input, &statsd.NoopClient{})
		if test.expectErr {
			if err == nil {
				t.Errorf("Failed case %s: expected error got nil", test.name)
				continue
			}
		}
		if err != nil && !test.expectErr {
			t.Errorf("Failed case %s: unexpected error %v", test.name, err)
			continue
		}
		if !reflect.DeepEqual(test.expectOut, out) {
			t.Errorf("Failed case %s: expected output %v got %v",
				test.name, test.expectOut, out)
		}
	}
}

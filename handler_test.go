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
		name             string
		input            []*scribe.LogEntry
		expectOut        *centrifugoRedisRequest
		expectBroadcasts int64
		expectErr        bool
	}

	now := time.Now()
	tests := []testCase{
		{
			name:  "Empty input",
			input: []*scribe.LogEntry{},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{},
			},
			expectBroadcasts: 0,
			expectErr:        false,
		},
		{
			name: "Single valid event input",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message:  "{\"channels\":[\"foo\"], \"data\":{\"foo\": 1234}}",
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "broadcast",
						Params: centrifugoBroadcastParams{
							Channels: []string{"foo"},
							Data:     json.RawMessage("{\"foo\": 1234}"),
						},
					},
				},
			},
			expectBroadcasts: 1,
			expectErr:        false,
		},
		{
			name: "Single valid event with OK TTL",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo\"], \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "broadcast",
						Params: centrifugoBroadcastParams{
							Channels: []string{"foo"},
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}",
								now.Unix())),
						},
					},
				},
			},
			expectBroadcasts: 1,
			expectErr:        false,
		},
		{
			name: "Single valid event with expired TTL",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo\"], \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Add(-1*time.Hour).Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{},
			},
			expectBroadcasts: 0,
			expectErr:        false,
		},
		{
			name: "Multiple events with OK TTL",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo\"], \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Add(-30*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo2\"], \"data\":{\"ts\": %d, \"ttl\":30, \"data\":{\"foo\": 5678}}}",
						now.Add(-20*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo3\", \"foo4\", \"foo5\"], \"data\":{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}}",
						now.Add(-2*time.Second).Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "broadcast",
						Params: centrifugoBroadcastParams{
							Channels: []string{"foo"},
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}",
								now.Add(-30*time.Second).Unix())),
						},
					},
					{
						Method: "broadcast",
						Params: centrifugoBroadcastParams{
							Channels: []string{"foo2"},
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":30, \"data\":{\"foo\": 5678}}",
								now.Add(-20*time.Second).Unix())),
						},
					},
					{
						Method: "broadcast",
						Params: centrifugoBroadcastParams{
							Channels: []string{"foo3", "foo4", "foo5"},
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}",
								now.Add(-2*time.Second).Unix())),
						},
					},
				},
			},
			expectBroadcasts: 5,
			expectErr:        false,
		},
		{
			name: "Multiple events with mixed TTL",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo\"], \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Add(-30*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo2\"], \"data\":{\"ts\": %d, \"ttl\":10, \"data\":{\"foo\": 5678}}}",
						now.Add(-20*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo3\"], \"data\":{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}}",
						now.Add(-2*time.Second).Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "broadcast",
						Params: centrifugoBroadcastParams{
							Channels: []string{"foo"},
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}",
								now.Add(-30*time.Second).Unix())),
						},
					},
					// event 2 expired
					{
						Method: "broadcast",
						Params: centrifugoBroadcastParams{
							Channels: []string{"foo3"},
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}",
								now.Add(-2*time.Second).Unix())),
						},
					},
				},
			},
			expectBroadcasts: 2,
			expectErr:        false,
		},
		{
			name: "Multiple events with mixed validity",
			input: []*scribe.LogEntry{
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo\"], \"data\":{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}}",
						now.Add(-30*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo2\"]}", // invalid no data field
						now.Add(-20*time.Second).Unix()),
				},
				{
					Category: "HUBD",
					Message: fmt.Sprintf("{\"channels\":[\"foo3\"], \"data\":{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}}",
						now.Add(-2*time.Second).Unix()),
				},
			},
			expectOut: &centrifugoRedisRequest{
				Data: []centrifugoApiCommand{
					{
						Method: "broadcast",
						Params: centrifugoBroadcastParams{
							Channels: []string{"foo"},
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":60, \"data\":{\"foo\": 1234}}",
								now.Add(-30*time.Second).Unix())),
						},
					},
					// event 2 is invalid
					{
						Method: "broadcast",
						Params: centrifugoBroadcastParams{
							Channels: []string{"foo3"},
							Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\":0, \"data\":{\"foo\": 9123}}",
								now.Add(-2*time.Second).Unix())),
						},
					},
				},
			},
			expectBroadcasts: 2,
			expectErr:        false,
		},
	}

	for _, test := range tests {
		out, totalBroadcast, err := scribeEntriesToBroadcastCommand(test.input, &statsd.NoopClient{})
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
		if totalBroadcast != test.expectBroadcasts {
			t.Errorf("Failed case %s: expected total broadcasts %v got %v",
				test.name, test.expectBroadcasts, totalBroadcast)
		}
	}
}

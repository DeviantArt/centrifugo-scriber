package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestParsingJSONMessage(t *testing.T) {
	type testCase struct {
		name          string
		input         string
		expectOut     *centrifugoPublishParams
		expectErr     bool
		expectErrType reflect.Type
	}
	// Deterministic time for all calls below
	now := time.Now()
	tests := []testCase{
		{
			name:          "Empty input",
			input:         "",
			expectOut:     nil,
			expectErr:     true,
			expectErrType: nil,
		},
		{
			name:          "Invalid JSON",
			input:         "{sadsada",
			expectOut:     nil,
			expectErr:     true,
			expectErrType: nil,
		},
		{
			name:          "Wrong format JSON - array",
			input:         "[\"asd\", 12354]",
			expectOut:     nil,
			expectErr:     true,
			expectErrType: nil,
		},
		{
			name:          "Wrong format JSON - object, wrong keys",
			input:         "{\"test\":12355, \"data\":{\"foo\":\"bar\"}}",
			expectOut:     nil,
			expectErr:     true,
			expectErrType: nil,
		},
		{
			name:  "Correct format JSON - no TTL",
			input: "{\"channel\":\"test\", \"data\":{\"foo\":\"bar\"}}",
			expectOut: &centrifugoPublishParams{
				Channel: "test",
				Data:    json.RawMessage("{\"foo\":\"bar\"}"),
			},
			expectErr:     false,
			expectErrType: nil,
		},
		{
			name: "Correct format JSON - expired TTL",
			input: fmt.Sprintf("{\"channel\":\"test\", \"data\":{\"ts\": %d, \"ttl\": 60, \"data\":{\"foo\":\"bar\"}}}",
				now.Add(-1*time.Hour).Unix()),
			expectOut:     nil,
			expectErr:     true,
			expectErrType: reflect.TypeOf(NewMessageStaleErr(uint32(time.Now().Add(-1*time.Hour).Unix()), 60, time.Now())),
		},
		{
			name: "Correct format JSON - Non expired TTL",
			input: fmt.Sprintf("{\"channel\":\"test\", \"data\":{\"ts\": %d, \"ttl\": 60, \"data\":{\"foo\":\"bar\"}}}",
				now.Add(-5*time.Second).Unix()),
			expectOut: &centrifugoPublishParams{
				Channel: "test",
				Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\": 60, \"data\":{\"foo\":\"bar\"}}",
					now.Add(-5*time.Second).Unix())),
			},
			expectErr:     false,
			expectErrType: nil,
		},
		{
			name: "Correct format JSON - 0 TTL",
			input: fmt.Sprintf("{\"channel\":\"test\", \"data\":{\"ts\": %d, \"ttl\": 0, \"data\":{\"foo\":\"bar\"}}}",
				now.Unix()),
			expectOut: &centrifugoPublishParams{
				Channel: "test",
				Data: json.RawMessage(fmt.Sprintf("{\"ts\": %d, \"ttl\": 0, \"data\":{\"foo\":\"bar\"}}",
					now.Unix())),
			},
			expectErr:     false,
			expectErrType: nil,
		},
	}

	for _, test := range tests {
		out, err := parseMessage([]byte(test.input))
		if test.expectErr {
			if err == nil {
				t.Errorf("Failed case %s: expected error got nil", test.name)
				continue
			}
			if test.expectErrType != nil && reflect.TypeOf(err) != test.expectErrType {
				t.Errorf("Failed case %s: expected error of type %v, got %v", test.expectErrType, err)
				continue
			}
		}
		if err != nil && !test.expectErr {
			t.Errorf("Failed case %s: unexpected error %v", test.name, err)
			continue
		}
		if !reflect.DeepEqual(test.expectOut, out) {
			t.Errorf("Failed case %s: expected output {%s, %s} got {%s, %s}",
				test.name, test.expectOut.Channel, test.expectOut.Data,
				out.Channel, out.Data)
		}
	}
}

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type MessageStaleErr struct {
	m string
}

func NewMessageStaleErr(ts, ttl uint32, processedAt time.Time) *MessageStaleErr {
	sent := time.Unix(int64(ts), 0)
	ttlDuration := time.Duration(ttl) * time.Second
	return &MessageStaleErr{
		fmt.Sprintf("Message expired %v ago: sent at %v, with ttl %v",
			processedAt.Sub(sent.Add(ttlDuration)), sent, ttlDuration),
	}
}

func (e *MessageStaleErr) Error() string {
	return e.m
}

// hubMessageMeta is the optional expected wrapping around each message payload.
// if message matches this, and the current time is later than ts + ttl then it is
// discarded instead of being delivered to centrifugo. Useful where downstream Scribe
// is configured to buffer on outage and might deliver a lot of very old stuff that is now
// irrelevant to users after outage.
type hubMessageMeta struct {
	TTL  uint32          `json:"ttl"`
	Ts   uint32          `json:"ts"`
	Data json.RawMessage `json:"data"`
}

type centrifugoBroadcastParams struct {
	Channels []string        `json:"channels"`
	Data     json.RawMessage `json:"data"`
}

type centrifugoApiCommand struct {
	Method string                    `json:"method"`
	Params centrifugoBroadcastParams `json:"params"`
}

type centrifugoRedisRequest struct {
	Data []centrifugoApiCommand `json:"data"`
}

// parseMessage attempts to parse an incoming raw JSON payload.
// On success it return a centrifugoPublishParams struct ready to be
// Marshalled to JSON. If there is an error parsing, or if the message TTL indicates
// it is stale, an nil is returned and error set.
func parseMessage(bytes []byte) (*centrifugoBroadcastParams, error) {
	var msg centrifugoBroadcastParams

	err := json.Unmarshal(bytes, &msg)
	if err != nil {
		return nil, err
	}

	// Sanity check it since Unmarshal doesn't require all struct fields to be set
	if len(msg.Channels) < 1 || len(msg.Data) < 1 {
		return nil, errors.New("No channel or data payload in message JSON")
	}

	// See if the Data payload is hub format with ts + ttl
	var meta hubMessageMeta
	err = json.Unmarshal(msg.Data, &meta)
	if _, ok := err.(*json.UnmarshalTypeError); ok {
		// Not in expected hub format, just continue anyway
		return &msg, nil
	}
	// Any other failure means message is just not valid JSON or something worse
	// fail whole operation don't even bother forwarding the msg with this payload embedded
	if err != nil {
		return nil, err
	}
	// Go json will unmarshal intoastruct even if not all fields are present so we need to check they are set
	// to non-zero values
	if meta.Ts == 0 || meta.TTL == 0 {
		// No Ts/TTL info in message (or TTL is 0 meaning it never expires), just forward it anyway
		return &msg, nil
	}

	// We have ts + ttl, check if message has expired
	now := time.Now()
	if int64(meta.Ts+meta.TTL) < now.Unix() {
		return nil, NewMessageStaleErr(meta.Ts, meta.TTL, now)
	}

	return &msg, nil
}

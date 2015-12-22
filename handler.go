package main

import (
	"encoding/json"
	"time"

	scribe "github.com/DeviantArt/centrifugo-scriber/gen-go/scribe"

	"github.com/DeviantArt/centrifugo-scriber/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/DeviantArt/centrifugo-scriber/Godeps/_workspace/src/github.com/quipo/statsd"
	"github.com/DeviantArt/centrifugo-scriber/Godeps/_workspace/src/gopkg.in/redis.v3"
)

// Handler Implements scribe.Scribe
type Handler struct {
	redisClient *redis.Client
	apiKey      string
	sd          statsd.Statsd
}

func NewHandler(redisAddr string, redisDB, redisIdleTimeout int, apiKey string, sd statsd.Statsd) (*Handler, error) {
	h := &Handler{
		apiKey: apiKey,
		sd:     sd,
	}
	h.redisClient = redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DB:           int64(redisDB),
		DialTimeout:  1 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		IdleTimeout:  time.Duration(redisIdleTimeout) * time.Second,
		MaxRetries:   3,
	})
	return h, nil
}

func scribeEntriesToBroadcastCommand(messages []*scribe.LogEntry, sd statsd.Statsd) (*centrifugoRedisRequest, int64, error) {
	var req centrifugoRedisRequest
	req.Data = make([]centrifugoApiCommand, 0, len(messages))

	var totalBroadcasts int64

	for _, m := range messages {
		msg, err := parseMessage([]byte(m.Message))
		if merr, ok := err.(*MessageStaleErr); ok {
			glog.Warningf("Dropping stale message: %s", merr)
			sd.Incr("dropped.stale_ttl", 1)
			continue
		}
		if err != nil {
			glog.Warningf("Failed to parse incoming JSON, Dropping message: %s, err: %s", m.Message, err)
			sd.Incr("dropped.invalid_format", 1)
			continue
		}

		totalBroadcasts += int64(len(msg.Channels))

		req.Data = append(req.Data, centrifugoApiCommand{
			Method: "broadcast",
			Params: *msg,
		})
	}

	return &req, totalBroadcasts, nil
}

func (h *Handler) Log(messages []*scribe.LogEntry) (r scribe.ResultCode, err error) {

	if len(messages) < 1 {
		return scribe.ResultCode_OK, nil
	}

	req, totalBroadcasts, err := scribeEntriesToBroadcastCommand(messages, h.sd)
	if err != nil {
		// Assume parse errors are fatal and client retry is pointless
		return scribe.ResultCode_OK, nil
	}

	if len(req.Data) < 1 {
		// Nothing to publish in this batch - all expired probably
		glog.Info("No publishable events in batch")
		return scribe.ResultCode_OK, nil
	}

	jsonBytes, err := json.Marshal(req)
	if err != nil {
		glog.Errorf("Failed to encode JSON body, dropping %d messages. err: %s", len(req.Data), err)
		// Still return OK since failing will just cause infinite retries...
		h.sd.Incr("error.encode_fail", 1)
		h.sd.Incr("dropped.encode_fail", int64(len(req.Data)))
		return scribe.ResultCode_OK, nil
	}

	jsonStr := string(jsonBytes)

	qSize, err := h.redisClient.RPush(h.apiKey, jsonStr).Result()
	if err != nil {
		glog.Errorf("Failed to push command to redis, downstream should retry. err: %s", err)
		h.sd.Incr("error.redis_publish_fail_temp", 1)
		return scribe.ResultCode_TRY_LATER, nil
	}
	h.sd.Incr("broadcasts", totalBroadcasts)
	h.sd.Incr("published", int64(len(req.Data)))
	h.sd.Gauge("centrifugo.api.queue_length", qSize)

	return scribe.ResultCode_OK, nil
}

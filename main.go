package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"gopkg.in/redis.v3"

	scribe "github.com/DeviantArt/centrifugo-scriber/gen-go/scribe"

	"github.com/apache/thrift/lib/go/thrift"
)

// Handler Implements scribe.Scribe
type Handler struct {
	redisClient *redis.Client
}

type centrifugoPublishParams struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type centrifugoApiCommand struct {
	Method string                  `json:"method"`
	Params centrifugoPublishParams `json:"params"`
}

type centrifugoRedisRequest struct {
	Data []centrifugoApiCommand `json:"data"`
}

func NewHandler(redisAddr string, redisDB int) (*Handler, error) {
	h := &Handler{}
	h.redisClient = redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DB:           int64(redisDB),
		DialTimeout:  1 * time.Second,
		ReadTimeout:  10 * time.Second, // Broadcasts to 50k users can take a while...
		WriteTimeout: 1 * time.Second,
	})
	return h, nil
}

func (h *Handler) Log(messages []*scribe.LogEntry) (r scribe.ResultCode, err error) {
	var req centrifugoRedisRequest
	req.Data = make([]centrifugoApiCommand, 0, len(messages))

	for _, m := range messages {
		var msg centrifugoPublishParams
		// Parse JSON message
		err := json.Unmarshal([]byte(m.Message), &msg)
		if err != nil {
			fmt.Errorf("Failed to parse incomoing JSON, Dropping message: %s, err: %s", m.Message, err)
			continue
		}

		req.Data = append(req.Data, centrifugoApiCommand{
			Method: "publish",
			Params: msg,
		})
	}

	jsonBytes, err := json.Marshal(req)
	if err != nil {
		fmt.Errorf("Failed to encode JSON body, dropping message. err: %s", err)
		// Still return OK since failing will just cause infinite retries...
		return scribe.ResultCode_OK, nil
	}

	jsonStr := string(jsonBytes)

	fmt.Println("Pushing", jsonStr)
	_, err = h.redisClient.RPush("centrifugo.api", jsonStr).Result()
	if err != nil {
		fmt.Errorf("Failed to push commands to redis will retry. err: %s", err)
		return scribe.ResultCode_TRY_LATER, nil
	}

	return scribe.ResultCode_OK, nil
}

func main() {
	var addr, redisAddr string
	var redisDB int

	flag.StringVar(&addr, "addr", "0.0.0.0:1463", "The host:port to listen on")
	flag.StringVar(&redisAddr, "redis", "localhost:6379", "The host:port to talk to redis on")
	flag.IntVar(&redisDB, "redis-db", 0, "Which redis DB to use")
	flag.Parse()

	var transport thrift.TServerTransport
	var err error

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())

	transport, err = thrift.NewTServerSocket(addr)
	if err != nil {
		panic(err)
	}

	handler, err := NewHandler(redisAddr, redisDB)
	if err != nil {
		panic(err)
	}

	processor := scribe.NewScribeProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("Starting the simple server... on ", addr)
	err = server.Serve()
	if err != nil {
		panic(err)
	}
}

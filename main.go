package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/DeviantArt/centrifugo-scriber/Godeps/_workspace/src/github.com/quipo/statsd"
	scribe "github.com/DeviantArt/centrifugo-scriber/gen-go/scribe"

	"github.com/DeviantArt/centrifugo-scriber/Godeps/_workspace/src/github.com/apache/thrift/lib/go/thrift"
)

func main() {
	var addr, redisAddr, apiKey, statsdHost, statsdPrefix string
	var redisDB, redisIdleTimeout, numPubShards int

	flag.StringVar(&addr, "addr", "0.0.0.0:1463",
		"The host:port to listen on")
	flag.StringVar(&redisAddr, "redis",
		"localhost:6379", "The host:port to talk to redis on")
	flag.IntVar(&redisDB, "redis-db", 0,
		"Which redis DB to use")
	flag.IntVar(&redisIdleTimeout, "redis-idle-timeout", 0,
		"How many seconds a redis connection can be idle before we recycle it. "+
			"If you have `timeout` config in your redis server config set to a non-zero value, this should be set lower. default 0")
	flag.StringVar(&apiKey, "centrifugo-api-key-pfx",
		"centrifugo.api", "Which redis key prefix the centrifugo API is looking in for publish queues")
	flag.IntVar(&numPubShards, "centrifugo-api-num-pub-shards",
		0, "How many shards cewntrifugo is looking in for high-throughput publish queues. "+
			"Default is 0 which means just use the single default API queue.")
	flag.StringVar(&statsdHost, "statsd-host", "",
		"hostname:port for statsd. If none given then metrics are not recorded")
	flag.StringVar(&statsdPrefix, "statsd-prefix", "centrifugo-scriber.",
		"Prefix for statsd metrics logged")
	flag.Parse()

	var statsdClient *statsd.StatsdClient
	var sd statsd.Statsd

	if len(statsdHost) > 0 {
		statsdClient = statsd.NewStatsdClient(statsdHost, statsdPrefix)
		statsdClient.CreateSocket()
		sd = statsd.NewStatsdBuffer(1*time.Second, statsdClient)
	} else {
		sd = &statsd.NoopClient{}
	}

	var transport thrift.TServerTransport
	var err error

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())

	transport, err = thrift.NewTServerSocket(addr)
	if err != nil {
		panic(err)
	}

	handler, err := NewHandler(redisAddr, redisDB, redisIdleTimeout, numPubShards, apiKey, sd)
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

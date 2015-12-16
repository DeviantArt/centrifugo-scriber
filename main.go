package main

import (
	"flag"
	"fmt"
	"time"

	scribe "github.com/DeviantArt/centrifugo-scriber/gen-go/scribe"
	"github.com/quipo/statsd"

	"github.com/apache/thrift/lib/go/thrift"
)

func main() {
	var addr, redisAddr, apiKey, statsdHost, statsdPrefix string
	var redisDB int

	flag.StringVar(&addr, "addr", "0.0.0.0:1463",
		"The host:port to listen on")
	flag.StringVar(&redisAddr, "redis",
		"localhost:6379", "The host:port to talk to redis on")
	flag.IntVar(&redisDB, "redis-db", 0,
		"Which redis DB to use")
	flag.StringVar(&apiKey, "centrifugo-api-key",
		"centrifugo.api", "Which redis key the centrifugo API is looking in for publish queue")
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

	handler, err := NewHandler(redisAddr, redisDB, apiKey, sd)
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

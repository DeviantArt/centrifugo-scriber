# centrifugo-scriber

A simple daemon to publish messages to the [Centrifugo real-time message server](https://github.com/centrifugal/centrifugo) from [Facebook Scribe](https://github.com/facebookarchive/scribe).

This allows you to publish high-throughput events from a non-daemon app (like PHP) to local Scribe and have them forwarded in a somewhat reliable manner to a pool of upstream `centrifugo-scriber` daemons. Each daemon published directly into `centrifugo` via it's [redis api](https://fzambia.gitbooks.io/centrifugal/content/server/engines.html).

## Features

 - Exports simple Scribe Thrift interface (without the fb303 service dependencies)
 - Supports reading TTL and timestamp from messages so old messages delivered late due to Scribe buffering can be dropped
 - If redis is not available, we fail with Scribe's `TRY_LATER` response so downstream Scribes will buffer and redeliver
 - Optionally log basic metrics about throughput and dropped messages to statsd
 - Saves a synchronous HTTP API call to publish each message to `centrifugo` during a web request

## Limitations

 - Requires using redis engine with centrifugo and enabling `-redis_api` option
 - No support for listing multiple channels in single message - Scribe transport is batched so it's pretty cheap to just write copies of message for each channel from app anyway. Might support this in future.

## Message Format

`LogEntry`s published to Scribe can have any `category`: `centrifugo-scriber` assumes Scribe category is only used for routing to this service.

The `message` payload must be JSON and must look like:

```json
{
    "channel": "foo",
    "data": {...}
}
```

Where `channel` is the `centrifugo` channel on which to publish the message, and data is arbitrary payload but MUST be a JSON object. The entire `data` value will be delivered via `centrifugo` to clients.

Optionally, you can encode `data` to have keys `ts` and `ttl` which should be UNIX timestamp (in seconds) the event occurred, and time to live in seconds respectively. If found, and non-zero, `centrifugo-scriber` will drop any messages that have already expired rather than publish them. A complete example is given below:

```json
{
    "channel": "foo",
    "data": {
        "ts": 1450286347,
        "ttl": 5,
        "mid": "ZGerM7jv",
        "data": {
            "count": 193
        }
    }
}
```

## Building

```
go get github.com/DeviantArt/centrifugo-scriber
```

## Running

```
$ ./centrifugo-scriber -h
Usage of centrifugo-scriber:
  -addr string
    	The host:port to listen on (default "0.0.0.0:1463")
  -alsologtostderr
    	log to standard error as well as files
  -centrifugo-api-key-pfx string
    	Which redis key prefix the centrifugo API is looking in for publish queues (default "centrifugo.api")
  -centrifugo-api-num-pub-shards int
    	How many shards cewntrifugo is looking in for high-throughput publish queues. Default is 0 which means just use the single default API queue.
  -log_backtrace_at value
    	when logging hits line file:N, emit a stack trace (default :0)
  -log_dir string
    	If non-empty, write log files in this directory
  -logtostderr
    	log to standard error instead of files
  -redis string
    	The host:port to talk to redis on (default "localhost:6379")
  -redis-db int
    	Which redis DB to use
  -redis-idle-timeout timeout
    	How many seconds a redis connection can be idle before we recycle it. If you have timeout config in your redis server config set to a non-zero value, this should be set lower. default 0
  -statsd-host string
    	hostname:port for statsd. If none given then metrics are not recorded
  -statsd-prefix string
    	Prefix for statsd metrics logged (default "centrifugo-scriber.")
  -stderrthreshold value
    	logs at or above this threshold go to stderr
  -v value
    	log level for V logs
  -vmodule value
    	comma-separated list of pattern=N settings for file-filtered logging
```

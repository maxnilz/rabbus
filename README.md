## Rabbus 🚌 ✨

* A tiny wrapper over [amqp](https://github.com/streadway/amqp) exchanges and queues.
* Support listen & serve multiple consumers at same via register handler function.
* Support produce message in both sync & async mode.
* In memory retries with exponential backoff for sending messages for async producing mode.
* Protect async producer calls with [circuit breaker](https://github.com/sony/gobreaker) for async producing mode.
* Automatic reconnect to RabbitMQ broker when connection is lost.
* Go channel API.

## Installation
```bash
go get -u github.com/maxnilz/rabbus
```

## Usage
The rabbus package exposes an interface for emitting and listening RabbitMQ messages.

### Emit in sync mode
```go
package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/maxnilz/rabbus"
	"github.com/sirupsen/logrus"
)

var (
	rabbusDsn = "amqp://localhost:5672"
)

func main() {
	exchange := flag.String("ex", "producer_test_ex", "exchange name")
	kind := flag.String("kind", "direct", "kind")
	key := flag.String("routing-key", "producer_test_key", "routing key")
	payload := flag.String("payload", "foo", "payload")
	flag.Parse()

	r, err := rabbus.New(
		rabbusDsn,
		rabbus.Durable(true),
		rabbus.WithLogger(logrus.StandardLogger()),
		rabbus.ChannelPoolSize(runtime.NumCPU()*5),
	)
	if err != nil {
		log.Fatalf("Failed to init rabbus connection %s", err)
		return
	}

	defer func(r *rabbus.Rabbus) {
		if err := r.Close(); err != nil {
			log.Fatalf("Failed to close rabbus connection %s", err)
		}
	}(r)

	msg := rabbus.Message{
		Exchange:     *exchange,
		Kind:         *kind,
		Key:          *key,
		Payload:      []byte(*payload),
		DeliveryMode: rabbus.Persistent,
	}

	if err := r.EmitSync(&msg); err != nil {
		log.Fatalln(err)
	}
}
```

### Listen & serve for multiple consumers at same time
```go
package main

import (
	"context"
	"log"
	"runtime"
	"time"

	"github.com/maxnilz/rabbus"
	"github.com/sirupsen/logrus"
)

var (
	rabbusDsn = "amqp://localhost:5672"
)

func main() {
	r, err := rabbus.New(
		rabbusDsn,
		rabbus.Durable(true),
		rabbus.WithLogger(logrus.StandardLogger()),
		rabbus.ChannelPoolSize(runtime.NumCPU()*5),
	)
	if err != nil {
		log.Fatalf("Failed to init rabbus connection %s", err)
		return
	}

	defer func(r *rabbus.Rabbus) {
		if err := r.Close(); err != nil {
			log.Fatalf("Failed to close rabbus connection %s", err)
		}
	}(r)

	declareArgs := rabbus.NewDeclareArgs().WithMessageTTL(time.Second * 120)

	r.HandleFunc(rabbus.ListenConfig{
		Exchange:        "consumer_test_ex_01",
		Kind:            rabbus.ExchangeDirect,
		Key:             "consumer_test_key_01",
		PassiveExchange: false,
		Queue:           "consumer_test_q_01",
		DeclareArgs:     declareArgs,
		BindArgs:        nil,
	}, func(ctx context.Context, message rabbus.ConsumerMessage) {
		log.Println("Message on queue 01 was consumed")
		message.Ack(false)
	})

	r.HandleFunc(rabbus.ListenConfig{
		Exchange:        "consumer_test_ex_02",
		Kind:            rabbus.ExchangeTopic,
		Key:             "consumer_test_key_02",
		PassiveExchange: false,
		Queue:           "consumer_test_q_02",
		DeclareArgs:     declareArgs,
		BindArgs:        nil,
	}, func(ctx context.Context, message rabbus.ConsumerMessage) {
		log.Println("Message on queue 02 was consumed")
		message.Ack(false)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := r.ListenAndServe(ctx); err != nil {
		log.Fatalln(err)
	}
}
```

## Contributing
- Fork it
- Create your feature branch (`git checkout -b my-new-feature`)
- Commit your changes (`git commit -am 'Add some feature'`)
- Push to the branch (`git push origin my-new-feature`)
- Create new Pull Request

## Badges

[![Go Report Card](https://goreportcard.com/badge/github.com/maxnilz/rabbus)](https://goreportcard.com/report/github.com/maxnilz/rabbus)
[![Go Doc](https://godoc.org/github.com/maxnilz/rabbus?status.svg)](https://godoc.org/github.com/maxnilz/rabbus)


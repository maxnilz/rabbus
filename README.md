## Rabbus 🚌 ✨

* A tiny wrapper over [amqp](https://github.com/streadway/amqp) exchanges and queues.
* In memory retries with exponential backoff for sending messages.
* Protect producer calls with [circuit breaker](https://github.com/sony/gobreaker).
* Automatic reconnect to RabbitMQ broker when connection is lost.
* Go channel API.

## Installation
```bash
go get -u github.com/maxnilz/rabbus
```

## Usage
The rabbus package exposes an interface for emitting and listening RabbitMQ messages.

### Emit
```go
import (
	"context"
	"time"

	"github.com/maxnilz/rabbus"
)

func main() {
	timeout := time.After(time.Second * 3)
	cbStateChangeFunc := func(name, from, to string) {
		// do something when state is changed
	}
	r, err := rabbus.New(
		rabbusDsn,
		rabbus.Durable(true),
		rabbus.Attempts(5),
		rabbus.Sleep(time.Second*2),
		rabbus.Threshold(3),
		rabbus.OnStateChange(cbStateChangeFunc),
	)
	if err != nil {
		// handle error
	}

	defer func(r Rabbus) {
		if err := r.Close(); err != nil {
			// handle error
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

	msg := rabbus.Message{
		Exchange: "test_ex",
		Kind:     "topic",
		Key:      "test_key",
		Payload:  []byte(`foo`),
	}

	r.EmitAsync() <- msg

	for {
		select {
		case <-r.EmitOk():
			// message was sent
		case <-r.EmitErr():
			// failed to send message
		case <-timeout:
			// handle timeout error
		}
	}
}
```

### Listen
```go
import (
	"context"
	"encoding/json"
	"time"

	"github.com/maxnilz/rabbus"
)

func main() {
	timeout := time.After(time.Second * 3)
	cbStateChangeFunc := func(name, from, to string) {
		// do something when state is changed
	}
	r, err := rabbus.New(
		rabbusDsn,
		rabbus.Durable(true),
		rabbus.Attempts(5),
		rabbus.Sleep(time.Second*2),
		rabbus.Threshold(3),
		rabbus.OnStateChange(cbStateChangeFunc),
	)
	if err != nil {
		// handle error
	}

	defer func(r Rabbus) {
		if err := r.Close(); err != nil {
			// handle error
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

	messages, err := r.Listen(rabbus.ListenConfig{
		Exchange:    "events_ex",
		Kind:        "topic",
		Key:         "events_key",
		Queue:       "events_q",
		DeclareArgs: rabbus.NewDeclareArgs().WithMessageTTL(15 * time.Minute).With("foo", "bar"),
		BindArgs:    rabbus.NewBindArgs().With("baz", "qux"),
	})
	if err != nil {
		// handle errors during adding listener
	}
	defer close(messages)

	go func(messages chan ConsumerMessage) {
		for m := range messages {
			m.Ack(false)
		}
	}(messages)
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


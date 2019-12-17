package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/maxnilz/rabbus"
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
		log.Fatalf("Failed to init rabbus connection %s", err)
		return
	}

	defer func(r *rabbus.Rabbus) {
		if err := r.Close(); err != nil {
			log.Fatalf("Failed to close rabbus connection %s", err)
		}
	}(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.Run(ctx)

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

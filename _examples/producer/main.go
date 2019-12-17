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

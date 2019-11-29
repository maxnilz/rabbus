package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/maxnilz/rabbus"
)

var (
	rabbusDsn = "amqp://localhost:5672"
	timeout   = time.After(time.Second * 3)
	wg        sync.WaitGroup
)

func main() {
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

	r.HandleFunc(rabbus.ListenConfig{
		Exchange:        "consumer_test_ex",
		Kind:            "direct",
		Key:             "consumer_test_key",
		PassiveExchange: false,
		Queue:           "consumer_test_q",
		DeclareArgs:     nil,
		BindArgs:        nil,
	}, func(ctx context.Context, message rabbus.ConsumerMessage) {
		log.Println("Message was consumed")
		message.Ack(false)
	})

	r.ListenAndServe(ctx)
}

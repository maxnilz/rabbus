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
		Kind:            rabbus.ExchangeTopic,
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

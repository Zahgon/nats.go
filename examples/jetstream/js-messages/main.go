package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST_STREAM",
		Subjects: []string{"FOO.*"},
	})
	if err != nil {
		log.Fatal(err)
	}

	cons, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "TestConsumerMessages",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}
	go endlessPublish(ctx, nc, js)

	it, err := cons.Messages(jetstream.PullMaxMessages(1))
	if err != nil {
		log.Fatal(err)
	}
	for {
		msg, err := it.Next()
		if err != nil {
			fmt.Println("next err: ", err)
		}
		fmt.Println(string(msg.Data()))
		msg.Ack()
	}
}

func endlessPublish(ctx context.Context, nc *nats.Conn, js jetstream.JetStream) {
	_ = "STUB: not implemented"
	return
}

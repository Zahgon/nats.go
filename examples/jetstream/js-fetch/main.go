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

	nc, err := nats.Connect("nats://127.0.0.1:4222")
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
		Durable:   "TestConsumerListener",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}
	go endlessPublish(ctx, nc, js)

	for {
		msgs, err := cons.Fetch(100, jetstream.FetchMaxWait(1*time.Second))
		if err != nil {
			fmt.Println(err)
		}
		for msg := range msgs.Messages() {
			fmt.Println(string(msg.Data()))
			msg.Ack()
		}
		if msgs.Error() != nil {
			fmt.Println("Error fetching messages: ", err)
		}
	}
}

func endlessPublish(ctx context.Context, nc *nats.Conn, js jetstream.JetStream) {
	_ = "STUB: not implemented"
	return
}

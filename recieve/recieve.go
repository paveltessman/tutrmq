package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const brokerURI = "amqp://guest:guest@localhost:5672/"
const queueAddr = "helow"

func releaseResource(fn func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := fn(ctx); err != nil {
		fmt.Println(err)
	}
}

func main() {
	ctx := context.Background()

	env := rmq.NewEnvironment(brokerURI, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		panic(err)
	}
	defer releaseResource(env.CloseConnections)

	_, err = conn.Management().DeclareQueue(ctx, &rmq.QuorumQueueSpecification{Name: queueAddr})

	consumer, err := conn.NewConsumer(ctx, queueAddr, nil)
	if err != nil {
		panic(err)
	}

	defer releaseResource(consumer.Close)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	for {
		delivery, err := consumer.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			panic(err)
		}

		msg := delivery.Message()

		var body string
		if len(msg.Data) > 0 {
			body = string(msg.Data[0])
		}
		fmt.Println(body)

		err = delivery.Accept(ctx)
		if err != nil {
			panic(err)
		}
	}
}

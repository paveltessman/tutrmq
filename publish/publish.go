package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const brokerURI = "amqp://guest:guest@localhost:5672/"
const xchange = "logs"

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

	_, err = conn.Management().DeclareExchange(ctx, &rmq.FanOutExchangeSpecification{Name: xchange})
	if err != nil {
		panic(err)
	}

	publisher, err := conn.NewPublisher(ctx, &rmq.ExchangeAddress{Exchange: xchange, Key: ""}, nil)
	if err != nil {
		panic(err)
	}

	defer releaseResource(publisher.Close)

	for {
		var body []byte
		fmt.Print("> ")
		_, err := fmt.Scan(&body)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			panic(err)
		}
		if len(body) == 0 {
			continue
		}

		res, err := publisher.Publish(ctx, rmq.NewMessage(body))
		if err != nil {
			panic(err)
		}

		switch res.Outcome.(type) {
		case *rmq.StateAccepted:
		default:
			log.Panicf("Unexpected outcome: %v", res.Outcome)
		}
	}
}

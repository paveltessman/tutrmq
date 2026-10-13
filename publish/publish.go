package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
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

func logLevel(args []string) string {
	levels := map[string]struct{}{"info": struct{}{}, "warn": struct{}{}, "error": struct{}{}}
	if len(args) < 2 {
		log.Panic("Please provide log level and message")
	}

	level := args[0]
	_, ok := levels[level]
	if !ok {
		log.Panic("fuk")
	}
	return level
}

func main() {
	ctx := context.Background()

	args := os.Args[1:]

	level := logLevel(args)

	env := rmq.NewEnvironment(brokerURI, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		panic(err)
	}
	defer releaseResource(env.CloseConnections)

	_, err = conn.Management().DeclareExchange(ctx, &rmq.DirectExchangeSpecification{Name: xchange})
	if err != nil {
		panic(err)
	}

	publisher, err := conn.NewPublisher(ctx, &rmq.ExchangeAddress{Exchange: xchange, Key: level}, nil)
	if err != nil {
		panic(err)
	}
	defer releaseResource(publisher.Close)

	body := []byte(strings.Join(args[1:], " "))
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

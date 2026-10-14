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

type logRecord struct {
	source string
	level  string
	msg    string
}

func (r *logRecord) key() string {
	return strings.Join([]string{r.source, r.level}, ".")
}

func (r *logRecord) body() []byte {
	return []byte(r.msg)
}

func newRecord(args []string) logRecord {
	if len(args) < 3 {
		log.Panic("Please provide source, log level and message")
	}

	levels := map[string]struct{}{"info": {}, "warn": {}, "error": {}}

	level := args[1]
	_, ok := levels[level]
	if !ok {
		log.Panic("fuk")
	}

	r := logRecord{source: args[0], level: level, msg: strings.Join(args[2:], " ")}

	return r
}

func main() {
	record := newRecord(os.Args[1:])

	ctx := context.Background()

	env := rmq.NewEnvironment(brokerURI, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		panic(err)
	}
	defer releaseResource(env.CloseConnections)

	_, err = conn.Management().DeclareExchange(ctx, &rmq.TopicExchangeSpecification{Name: xchange})
	if err != nil {
		panic(err)
	}

	publisher, err := conn.NewPublisher(ctx, &rmq.ExchangeAddress{Exchange: xchange, Key: record.key()}, nil)
	if err != nil {
		panic(err)
	}
	defer releaseResource(publisher.Close)

	res, err := publisher.Publish(ctx, rmq.NewMessage(record.body()))
	if err != nil {
		panic(err)
	}

	switch res.Outcome.(type) {
	case *rmq.StateAccepted:
	default:
		log.Panicf("Unexpected outcome: %v", res.Outcome)
	}
}

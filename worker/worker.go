package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const brokerURI = "amqp://guest:guest@localhost:5672/"
const queueAddr = "helow"

func releaseResourse(fn func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := fn(ctx); err != nil {
		fmt.Println(err)
	}
}

func spawn(ctx context.Context, id int) {
	env := rmq.NewEnvironment(brokerURI, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		panic(err)
	}
	defer releaseResourse(env.CloseConnections)

	_, err = conn.Management().DeclareQueue(ctx, &rmq.QuorumQueueSpecification{Name: queueAddr})

	consumer, err := conn.NewConsumer(ctx, queueAddr, &rmq.ConsumerOptions{InitialCredits: 1})
	if err != nil {
		panic(err)
	}

	defer releaseResourse(consumer.Close)

	log.Printf(" [%d] waiting for messages", id)

	for {
		delivery, err := consumer.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			panic(err)
		}

		msg := delivery.Message()

		var payload []byte
		if len(msg.Data) > 0 {
			payload = msg.Data[0]
		}

		log.Printf("[%d] recieved: %s", id, payload)

		dotCount := bytes.Count(payload, []byte("."))
		dur := time.Duration(dotCount)
		time.Sleep(dur * time.Second)

		log.Printf("[%d] done: %s", id, payload)

		err = delivery.Accept(ctx)
		if err != nil {
			panic(err)
		}
	}
	log.Printf(" [%d] shutting down", id)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	for i := range 3 {
		wg.Go(func() {
			spawn(ctx, i)
		})
	}

	wg.Wait()
}

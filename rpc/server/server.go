package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	amqp "github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const brokerURI = "amqp://guest:guest@localhost:5672/"
const qName = "rpc_queue"

func fib(n int) int {
	if n <= 2 {
		return 1
	}
	return fib(n-1) + fib(n-2)
}

func handleRequest(_ context.Context, request *amqp.Message, workerID int) (*amqp.Message, error) {
	var payload []byte
	if len(request.Data) > 0 {
		payload = request.Data[0]
	}

	n, err := strconv.Atoi(string(payload))
	if err != nil {
		return nil, err
	}

	log.Printf("[%d] counting %d...", workerID, n)
	result := fib(n)
	log.Printf("[%d] done: %d", workerID, result)

	return amqp.NewMessage([]byte(strconv.Itoa(result))), nil

}

func releaseResource(fn func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := fn(ctx); err != nil {
		fmt.Println(err)
	}
}

func worker(ctx context.Context, conn *rmq.AmqpConnection, workerID int) {

	handler := func() rmq.ResponderHandler {
		return func(ctx context.Context, request *amqp.Message) (*amqp.Message, error) {
			return handleRequest(ctx, request, workerID)
		}
	}

	responder, err := conn.NewResponder(ctx, rmq.ResponderOptions{
		RequestQueue: qName,
		Handler:      handler(),
	})
	if err != nil {
		panic(err)
	}
	defer releaseResource(responder.Close)

	log.Printf(" [%d] ready", workerID)

	<-ctx.Done()

	log.Printf(" [%d] shutting down", workerID)
}

func initQueue(ctx context.Context) (*rmq.Environment, *rmq.AmqpConnection, error) {
	env := rmq.NewEnvironment(brokerURI, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		return nil, nil, err
	}

	_, err = conn.Management().DeclareQueue(ctx, &rmq.QuorumQueueSpecification{
		Name: qName,
	})
	if err != nil {
		return nil, nil, err
	}

	_, err = conn.Management().PurgeQueue(ctx, qName)
	if err != nil {
		return nil, nil, err
	}
	return env, conn, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	env, conn, err := initQueue(ctx)
	if err != nil {
		panic(err)
	}
	defer releaseResource(env.CloseConnections)

	var wg sync.WaitGroup

	for i := range 3 {
		wg.Go(func() {
			worker(ctx, conn, i)
		})
	}

	wg.Wait()
}

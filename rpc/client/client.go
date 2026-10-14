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

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const brokerURI = "amqp://guest:guest@localhost:5672/"
const qName = "rpc_queue"

func releaseResource(fn func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := fn(ctx); err != nil {
		fmt.Println(err)
	}
}

func newPayload(args []string) []int {
	if len(args) < 2 {
		log.Panic("Please do not make mistakes")
	}

	parsed := make([]int, 0, len(args[1:]))
	for _, val := range args[1:] {
		n, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		parsed = append(parsed, n)
	}
	return parsed
}

func spawn(ctx context.Context, requester rmq.Requester, id int, n int) (int, error) {
	payload := []byte(strconv.Itoa(n))

	log.Printf("[%d] requesting: %d", id, n)

	reply, err := requester.Publish(ctx, requester.Message(payload))
	if err != nil {
		return 0, err
	}

	r := <-reply
	if r == nil {
		return 0, err
	}

	var data []byte
	if len(r.Data) > 0 {
		data = r.Data[0]
	}

	result, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, err
	}

	log.Printf("[%d] recieved: %d", id, result)

	return result, nil
}

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	env := rmq.NewEnvironment(brokerURI, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		panic(err)
	}
	defer releaseResource(env.CloseConnections)

	_, err = conn.Management().DeclareQueue(ctx, &rmq.QuorumQueueSpecification{Name: qName})
	if err != nil {
		panic(err)
	}

	requester, err := conn.NewRequester(ctx, &rmq.RequesterOptions{
		RequestQueueName: qName,
	})
	if err != nil {
		panic(err)
	}
	defer releaseResource(requester.Close)


	var wg sync.WaitGroup

	args := newPayload(os.Args)

	for i, n := range args {
		wg.Go(func() {
			_, err := spawn(ctx, requester, i, n)
			if err != nil {
				log.Println(err)
			}
		})
	}

	wg.Wait()
}

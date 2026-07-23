package main

import (
	"context"
	"log"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const brokerURI = "amqp://guest:guest@localhost:5672"

func main() {
	ctx := context.Background()
	env := rmq.NewEnvironment(brokerURI, nil)
	conn, err := env.NewConnection(ctx)
	if err != nil {
		log.Panicf("Failed to establish connection: %v", err)
	}

	defer func() {
		_ = env.CloseConnections(context.Background())
	}()

	_, err = conn.Management().DeclareQueue(ctx, &rmq.QuorumQueueSpecification{Name: "hello"})
	if err != nil {
		log.Panicf("Failed to create queue: %v", err)
	}

	publisher, err := conn.NewPublisher(ctx, &rmq.QueueAddress{Queue: "hello"}, nil)
	if err != nil {
		log.Panicf("Failed to create published: %v", err)
	}

	defer func() {
		_ = publisher.Close(context.Background())
	}()

	msg := "hello world"
	res, err := publisher.Publish(ctx, rmq.NewMessage([]byte(msg)))
	if err != nil {
		log.Panicf("Failed to publish message: %v", err)
	}

	switch res.Outcome.(type) {
	case *rmq.StateAccepted:
	default:
		log.Panicf("unexpected outcome: %v", res.Outcome)
	}

	log.Printf(" [X] sent msg: %s", msg)

}

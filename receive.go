package main

import (
	"context"
	"errors"
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

	consumer, err := conn.NewConsumer(ctx, "hello", nil)
	if err != nil {
		log.Panicf("Failed to create consumer: %v", err)
	}
	defer func() {
		_ = consumer.Close(context.Background())
	}()

	log.Printf(" [*] waiting for msg.")

	for {
		delivery, err := consumer.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Panicf("Failed to receive message: %v", err)
		}

		msg := delivery.Message()
		var body string
		if len(msg.Data) > 0 {
			body = string(msg.Data[0])
		}

		log.Printf("Received: %s", body)

		err = delivery.Accept(ctx)
		if err != nil {
			log.Panicf("Failed to accept message: %v", err)
		}

	}
}

package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitClient holds our connection infrastructure
type RabbitMQ struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
}

var ActiveRabbitMQ RabbitMQ

func RabbitClient() (*RabbitMQ, error) {
	protocol := os.Getenv("RABBITMQ_PROTOCOL")
	user := os.Getenv("RABBITMQ_USER")
	pass := os.Getenv("RABBITMQ_PASS")
	host := os.Getenv("RABBITMQ_HOST")
	port := os.Getenv("RABBITMQ_PORT")
	vhost := os.Getenv("RABBITMQ_VHOST")

	if protocol == "" || user == "" || pass == "" || host == "" || port == "" {
		return nil, fmt.Errorf("missing required RabbitMQ environment variables")
	}

	connStr := fmt.Sprintf("%s://%s:%s@%s:%s/%s", protocol, user, pass, host, port, vhost)

	log.Printf("Connecting to RabbitMQ host: %s...", host)

	conn, err := amqp.Dial(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	log.Println("Successfully connected to RabbitMQ and opened a channel!")

	var rabbitmq_reference = &RabbitMQ{
		Conn:    conn,
		Channel: ch,
	}

	ActiveRabbitMQ = *rabbitmq_reference
	
	return rabbitmq_reference, nil
}

func (rc *RabbitMQ) Publish(ctx context.Context, queueName string, body []byte) error {
	// Automatically declare the queue just in case it doesn't exist yet
	_, err := rc.Channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare queue on publish: %w", err)
	}

	return rc.Channel.PublishWithContext(ctx,
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json", // standard for modern APIs
			DeliveryMode: amqp.Persistent,     // makes message survive broker restarts
			Body:         body,
		},
	)
}

func (rc *RabbitMQ) StartListener(queueName string, handler func(body []byte)) error {
	_, err := rc.Channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare queue on listen: %w", err)
	}

	msgs, err := rc.Channel.Consume(
		queueName, "", true, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to consume queue: %w", err)
	}

	// Spin up a separate goroutine so it listens indefinitely in the background
	go func() {
		slog.Info("RabbitMQ listener started", "queue", queueName)
		for d := range msgs {
			handler(d.Body) // execute our custom business logic function
		}
	}()

	return nil
}

func (rc *RabbitMQ) Close() {
	if rc.Channel != nil {
		rc.Channel.Close()
	}
	if rc.Conn != nil {
		rc.Conn.Close()
	}
}
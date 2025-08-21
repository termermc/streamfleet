package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/termermc/streamfleet"
)

// EmailSenderWorker is a worker that sends emails that were put on the email queue.
type EmailSenderWorker struct {
	sfServer *streamfleet.Server
}

// NewEmailSenderWorker creates a new email sender worker with the specified Redis client connection options.
// The serverUniqueId field should be the machine's unique identifier, like its hostname.
func NewEmailSenderWorker(ctx context.Context, serverUniqueId string, redisOpt streamfleet.ToRedisClient) (*EmailSenderWorker, error) {
	sfServer, err := streamfleet.NewServer(ctx, streamfleet.ServerOpt{
		RedisOpt:       redisOpt,
		ServerUniqueId: serverUniqueId,
	})
	if err != nil {
		return nil, fmt.Errorf(`failed to create underlying work queue server: %w`, err)
	}

	sfServer.Handle(emailsQueueKey, func(ctx context.Context, task *streamfleet.Task) error {
		var email PendingEmail
		err = json.Unmarshal([]byte(task.Data), &email)
		if err != nil {
			return fmt.Errorf(`failed to unmarshal email: %w`, err)
		}

		fmt.Printf("---- INCOMING EMAIL ----\n")
		fmt.Printf("To: %s\n", email.Recipient)
		fmt.Printf("Subject: %s\n", email.Subject)
		fmt.Printf("------------------------\n")
		fmt.Printf("%s\n", email.Body)
		fmt.Printf("------------------------\n\n")

		return nil
	})

	return &EmailSenderWorker{
		sfServer: sfServer,
	}, nil
}

// Run starts the worker, blocking until it is closed.
func (c *EmailSenderWorker) Run() error {
	err := c.sfServer.Run()
	if err != nil {
		return fmt.Errorf(`failed to run underlying work queue server: %w`, err)
	}

	return nil
}

func (c *EmailSenderWorker) Close() error {
	if err := c.sfServer.Close(); err != nil {
		return fmt.Errorf(`failed to close underlying work queue server: %w`, err)
	}
	return nil
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/mail"
	"time"

	"github.com/termermc/streamfleet"
)

const emailExpireDuration = 1 * time.Hour

// EmailClient is an email client that uses a work queue to send emails on any available sender workers.
type EmailClient struct {
	sfClient *streamfleet.Client
}

// NewEmailClient creates a new email client using the specified Redis client connection options.
func NewEmailClient(ctx context.Context, redisOpt streamfleet.ToRedisClient) (*EmailClient, error) {
	sfClient, err := streamfleet.NewClient(ctx, streamfleet.ClientOpt{
		RedisOpt: redisOpt,
	}, emailsQueueKey)
	if err != nil {
		return nil, fmt.Errorf(`failed to create underlying work queue client: %w`, err)
	}

	return &EmailClient{
		sfClient: sfClient,
	}, nil
}

// SendEmail queues an email to be sent for the specified recipient.
// The email is sent asynchronously by a worker.
// To wait on the delivery of the email, call Wait() on the returned handle.
func (c *EmailClient) SendEmail(recipient *mail.Address, subject string, body string) (*streamfleet.TaskHandle, error) {
	pending := PendingEmail{
		Recipient: recipient.String(),
		Subject:   subject,
		Body:      body,
	}
	data, _ := json.Marshal(pending)

	handle, err := c.sfClient.EnqueueAndTrack(emailsQueueKey, string(data), streamfleet.TaskOpt{
		ExpiresTs: time.Now().Add(emailExpireDuration),
	})
	if err != nil {
		return nil, fmt.Errorf(`failed to enqueue email: %w`, err)
	}

	return handle, nil
}

func (c *EmailClient) Close() error {
	if err := c.sfClient.Close(); err != nil {
		return fmt.Errorf(`failed to close underlying work queue client: %w`, err)
	}
	return nil
}

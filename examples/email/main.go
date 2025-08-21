// Implementation of a mock email queue client and server.
// It is recommended to read through the simple example if you have not already.

package main

import (
	"context"
	"net/mail"

	"github.com/termermc/streamfleet"
)

// The queue key used for pending emails.
const emailsQueueKey = "emails"

// PendingEmail is a pending queued email.
// Note that in a real production environment, it would be wise to version serialized messages such as this.
type PendingEmail struct {
	Recipient string `json:"recipient"`
	Subject   string `json:"subject"`
	Body      string `json:"body"`
}

func main() {
	redisOpt := streamfleet.RedisClientOpt{
		Addr: "127.0.0.1:6379",
	}

	ctx := context.Background()

	// Client is instantiated before server in this example to illustrate the ability to queue work before a server is available.
	client, err := NewEmailClient(ctx, redisOpt)
	if err != nil {
		panic(err)
	}

	addr, err := mail.ParseAddress("sales@palantir.com")
	if err != nil {
		panic(err)
	}
	handle, err := client.SendEmail(addr, "Invoice", "When will I get my invoice for this dope and sick and awesome surveillance tool?\n\nAll the best, hugs and kisses,\nPeter Thiel's #1 Fan")

	worker, err := NewEmailSenderWorker(ctx, "node1.example.com", redisOpt)
	if err != nil {
		panic(err)
	}

	// Start the worker in a goroutine.
	go func() {
		err = worker.Run()
		if err != nil {
			panic(err)
		}
	}()

	// Wait for the email to be delivered.
	err = handle.Wait()
	if err != nil {
		panic(err)
	}
}

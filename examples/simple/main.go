package main

import (
	"context"
	"fmt"
	"time"

	"github.com/termermc/streamfleet"
)

func main() {
	var redisOpt = streamfleet.RedisClientOpt{
		Addr: "127.0.0.1:6379",
	}

	const MessagesQueueKey = "messages"

	ctx := context.Background()

	// Create server.
	// ServerUniqueId is used to differentiate servers from each other.
	// Some other options we could specify:
	//  - Logger (an implementation of slog.Logger from the standard library to override the default logger)
	//  - HandlerConcurrency (how many concurrent tasks the server can handle, defaults to 1)
	// Take a look at the ServerOpt struct for a complete list of options.
	server, err := streamfleet.NewServer(ctx, streamfleet.ServerOpt{
		ServerUniqueId: "node1.example.com",
		RedisOpt:       redisOpt,
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = server.Close()
	}()

	// Register handlers for queues.
	// This must be done before running the server.
	server.Handle(MessagesQueueKey, func(ctx context.Context, task *streamfleet.Task) error {
		fmt.Printf("Got message: %s\n", task.Data)
		return nil
	})

	// Launch server.
	// The Run() method blocks until the server is closed or runs into a fatal error, so it is launched in its own goroutine.
	// Note that servers do not need to be created before clients; queued messages will be picked up as soon as a server is available.
	go func() {
		err := server.Run()
		if err != nil {
			panic(err)
		}
	}()

	// Create a client using the same Redis connection options as the server.
	// Unlike servers, we do not need to manually provide a unique ID.
	// Instead, the client generates its own internally.
	// Take a look at the ClientOpt struct for a complete list of options.
	client, err := streamfleet.NewClient(ctx, streamfleet.ClientOpt{
		RedisOpt: redisOpt,
	}, MessagesQueueKey)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = client.Close()
	}()

	// Send some messages for the server to print.
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		// Here, we're using EnqueueAndForget which, as the name suggests, uses a fire-and-forget strategy.
		// If we wanted to know when a server successfully processed the task or ran into an error, we could have used EnqueueAndForget.
		// Some other options we could specify:
		//  - MaxRetries (the maximum number of times to retry the task on failure, defaults to 0 for infinite tries)
		//  - ExpiresTs (an expiration timestamp, at which time the task will be expired if not already processed)
		// Take a look at the TaskOpt struct for a complete list of options.
		err = client.EnqueueAndForget(MessagesQueueKey, "The current time is: "+time.Now().String(), streamfleet.TaskOpt{})
		if err != nil {
			panic(err)
		}
	}
}

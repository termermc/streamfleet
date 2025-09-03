# streamfleet

Customizable Go work queue implementation backed by Redis Streams (or Valkey
Streams). No Lua or fancy tricks required.

Tasks placed on the queue are processed once by the worker that can pick up the
task quickest. Completion can be optionally tracked.

## Features

- Optional task completion notifications
- Support for Redis Sentinel and Cluster
- Automatic retry and worker crash recovery
- Tolerance for Redis downtime without data loss

## Architecture

The library is broken up into two components, coordinated by Redis:

- The client, which submits (enqueues) tasks to the queue.

  Clients can optionally track completion or failure of tasks by listening for
  notifications about them. Whether to send completion notifications is sent
  along with the task.

- The server (worker), which accepts work from the queue and processes it.

  In addition to receiving new tasks, workers can claim tasks that have sat idle
  too long in other workers' pending lists. Long-running tasks are kept fresh
  and not in an idle state as long as the task is pending in the functional
  server. Only servers that have crashed or are unresponsive would have their
  tasks reclaimed.

  If a task fails while the worker is running, the server will put the task back
  on the queue and increment its failure count.

Note that servers and clients may exist on the same process; they do not need to
be in separate microservices.

Task notifications are sent over a client-specific stream for reliable delivery,
even if there is temporary disconnection from Redis. Clients clean up these
streams when their Close method is called to avoid resource leaks. In the event
that they are not closed cleanly, other clients and servers use a simple garbage
collector based on client heartbeats to find orphaned client streams and delete
them.

If the underlying Redis server is unavailable, the clients will wait until it
comes back online. Tasks submitted to the client stay queued in-memory until
Redis is available.

The overall design philosophy is to support queues with high error tolerance
without losing anything, and to have as few moving parts as possible. Many
systems already have Redis, why not use its stream feature for reliable message
delivery? No need to introduce a complex system like Kafka. As well as being
fault-tolerance, the implementation aims to be as autonomous as possible. There
are no features that rely on centralized coordination, other than Redis itself
(which can be clustered for high-availability).

## Use It

To add the library to your project, run:

```bash
go get github.com/termermc/streamfleet
```

To see some examples, visit the [examples](./examples) directory.

### Simple Example

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/termermc/streamfleet"
)

func main() {
	redisOpt := streamfleet.RedisClientOpt{
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
```

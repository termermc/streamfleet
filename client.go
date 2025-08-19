package streamfleet

import (
	"context"
	"fmt"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/redis/go-redis/v9"
)

// TODO On the client side, enforce expiration for tracked tasks.
// This means pending tasks should be failed with an expired error if a reply has not been received.

const clientGroupName = "receiver"

// ClientOpt are options for Streamfleet clients.
type ClientOpt struct {
	// The Redis client options to use.
	// See the RedisClientOpt, RedisClusterClientOpt or RedisClusterClientOpt struct for more details.
	//
	// Required.
	RedisOpt ToRedisClient

	// The timeout to use for reconnections to Redis in the event disconnection.
	// If unspecified, there is no timeout and the server will attempt to reconnect forever.
	RetryTimeout time.Duration

	// The maximum number of tasks to queue locally in-memory while waiting for Redis to come back online.
	// After the local queue has filled up, new tasks being queued will result in an error.
	// Defaults to 10.
	MaxLocalQueueSize int

	// TODO Pluggable logger
}

// Client is a work queue client.
// It submits tasks to the queue and can receive notifications of their completion.
type Client struct {
	// The client's unique ID.
	id string

	// Whether the server is running.
	isRunning bool

	// Mapping of queue keys to their underlying stream keys.
	queueToStream map[string]string

	// Client options.
	opt ClientOpt

	// The underlying Redis client.
	client redis.UniversalClient

	// Locally queued tasks to be submitted to the queue.
	queuedTasks chan queuedTask

	// A mapping of task IDs to their corresponding handles.
	pendingTasks *xsync.Map[string, *TaskHandle]
}

// ErrNoClientQueueKeys is returned when calling NewClient without specifying any queue keys for the client.
var ErrNoClientQueueKeys = fmt.Errorf("streamfleet: no queue keys were specified when calling NewClient")

// NewClient creates a new client instance.
// The list of queue keys to support for this client must be exhaustive and cannot change without creating a new client.
// Returns ErrNoClientQueueKeys if no queue keys were specified.
func NewClient(ctx context.Context, opt ClientOpt, queueKeys ...string) (*Client, error) {
	if len(queueKeys) == 0 {
		return nil, ErrNoClientQueueKeys
	}

	id := MustUuidV7()

	client := opt.RedisOpt.ToClient()

	queueToStream := make(map[string]string)
	for _, key := range queueKeys {
		queueToStream[key] = KeyPrefix + key
	}

	if opt.MaxLocalQueueSize < 1 {
		opt.MaxLocalQueueSize = 10
	}

	c := &Client{
		id:            id,
		isRunning:     true,
		opt:           opt,
		queueToStream: queueToStream,
		client:        client,
		queuedTasks:   make(chan queuedTask, opt.MaxLocalQueueSize),
		pendingTasks:  xsync.NewMap[string, *TaskHandle](),
	}

	// Create stream and consumer group.
	err := client.XGroupCreateMkStream(ctx, mkRecvStreamKey(id), clientGroupName, "0").Err()
	if err != nil {
		return nil, fmt.Errorf(`streamfleet: failed to create receiver for client with ID %s while instantiating with NewClient: %w`, id, err)
	}

	// Create heartbeat list entries.
	if err = c.doHeartbeat(ctx); err != nil {
		return nil, err
	}

	go c.heartbeatLoop()

	return c, nil
}

func (c *Client) doHeartbeat(ctx context.Context) error {
	for queueKey := range c.queueToStream {
		hbKey := mkRecvHeartbeatKey(queueKey)
		err := c.client.ZAdd(ctx, hbKey, redis.Z{
			Score:  float64(time.Now().UnixMilli()),
			Member: c.id,
		}).Err()
		if err != nil {
			return fmt.Errorf(`streamfleet: failed to create heartbeat key for client with ID %s for work queue %s: %w`, c.id, queueKey, err)
		}
	}

	return nil
}

func (c *Client) heartbeatLoop() {
	ctx := context.Background()

	for c.isRunning {
		time.Sleep(receiverStreamHeartbeatInterval)
		if !c.isRunning {
			break
		}

		if err := c.doHeartbeat(ctx); err != nil {
			// TODO Log error
			continue
		}
	}
}

// ErrUnsupportedQueueKey is returned when trying to enqueue a task for a queue key that the client was not instantiated with.
var ErrUnsupportedQueueKey = fmt.Errorf(`streamfleet: unsupported queue key, client was not instantiated with support for this queue key`)

// EnqueueAndForget adds a task to the queue and immediately returns.
// Errors in queuing or the status of the task once it is picked up by a server are not tracked.
// If you need to know when the task has been completed, use EnqueueAndTrack.
func (c *Client) EnqueueAndForget(queueKey string, data []byte, opt TaskOpt) error {
	if !c.isRunning {
		panic(fmt.Sprintf("streamfleet: tried to queue task on closed server (queueKey: %s)", queueKey))
	}

	if _, hasQueue := c.queueToStream[queueKey]; !hasQueue {
		return ErrUnsupportedQueueKey
	}

	task := newTask(data, false, opt)

	c.queuedTasks <- queuedTask{
		Id:     task.Id,
		Stream: KeyPrefix + queueKey,
		Queue:  queueKey,
		Task:   task,
	}

	return nil
}

// EnqueueAndTrack adds a task to the queue and returns a handle to it.
// Errors in queuing or the status of the task once it is picked up by the server can be tracked using the returned handle.
// If you do not need to know when the task has been completed, use EnqueueAndForget.
// You should only use this method if you need to know the status of the task, as it is less efficient than EnqueueAndForget.
func (c *Client) EnqueueAndTrack(queueKey string, data []byte, opt TaskOpt) (*TaskHandle, error) {
	if !c.isRunning {
		panic(fmt.Sprintf("streamfleet: tried to queue task on closed server (queueKey: %s)", queueKey))
	}

	stream, hasQueue := c.queueToStream[queueKey]
	if !hasQueue {
		return nil, ErrUnsupportedQueueKey
	}

	task := newTask(data, true, opt)
	taskHandle := &TaskHandle{
		Id:         task.Id,
		resultChan: make(chan error, 1),
	}
	c.pendingTasks.Store(task.Id, taskHandle)
	c.queuedTasks <- queuedTask{
		Id:     task.Id,
		Stream: stream,
		Queue:  queueKey,
		Task:   task,
	}

	return taskHandle, nil
}

func (c *Client) enqueueLoop() {
	for queued := range c.queuedTasks {
		if !c.isRunning {
			// TODO Mark task as canceled and log
			continue
		}

		pending, hasP := c.pendingTasks.Load(queued.Task.Id)

		retry := func() {
			if len(c.queuedTasks) >= cap(c.queuedTasks) {
				// TODO Error, also putting reschedule error in task handle if present
				// Log failure to requeue.
				// Maybe even move this to a dedicated method.
			} else {
				c.queuedTasks <- queued
			}
		}

		if queued.Task.ExpiresTs.After(time.Now()) {
			if hasP {
				pending.resultChan <- ErrTaskExpired
			}

			// TODO Should I log here? Maybe have a setting about logging expired tasks?
		}

		err := c.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: queued.Stream,
			Values: queued.Task.encode(),

			// TODO Should I use MAXLEN here?
			// What other options are needed?
			// Consult Redis docs.
		})
		if err != nil {
			// TODO Log error

			retry()
			continue
		}
	}
}

// Listens for pending task notifications and notifies pending task handles.
func (c *Client) notifSubscriber() {
	ctx := context.Background()

	// TODO Do trimming after receiving new messages (trim IDs below the one received)

	for c.isRunning {
		stream := mkRecvStreamKey(c.id)
		streamResults, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    clientGroupName,
			Streams:  []string{stream},
			Consumer: c.id,
			Block:    0,
			Count:    10,
		}).Result()
		if err != nil {
			// TODO If this is due to the stream or group not existing, recreate it and try again.
			// TODO Log this
			err = fmt.Errorf("streamfleet: server failed to read from Redis: %w", err)
			continue
		}

		// No results.
		// There should be exactly one stream.
		if len(streamResults) == 0 || len(streamResults[0].Messages) == 0 {
			continue
		}

		msgs := streamResults[0].Messages
		for _, msg := range msgs {
			var notif *TaskNotification
			notif, err = decodeTaskNotification(msg.Values)
			if err != nil {
				// TODO Log error about failing to decode task notification
				continue
			}

			pending, hasP := c.pendingTasks.LoadAndDelete(notif.TaskId)
			if !hasP {
				continue
			}

			switch notif.Type {
			case TaskNotificationTypeCompleted:
				pending.resultChan <- nil
			case TaskNotificationTypeCanceled:
				pending.resultChan <- ErrTaskCanceled
			case TaskNotificationTypeExpired:
				pending.resultChan <- ErrTaskExpired
			case TaskNotificationTypeError:
				pending.resultChan <- fmt.Errorf(`streamfleet: task failed with error: %s`, notif.ErrMsg)
			default:
				// TODO Log unknown notification type
			}
		}

		// Trim stream before the last received message .
		lastMsg := msgs[len(msgs)-1]
		err = c.client.XTrimMinID(ctx, stream, lastMsg.ID).Err()
		if err != nil {
			// TODO Log error
			err = fmt.Errorf(`streamfleet: failed to trim stream to min ID %s: %w`, lastMsg.ID, err)
			continue
		}
	}
}

// Close closes resources associated with the client.
// Will cancel pending tasks. Locally queued tasks will be lost.
// The client must not be used after calling Close.
func (c *Client) Close() error {
	c.isRunning = false

	close(c.queuedTasks)

	// Cancel tracked pending tasks.
	c.pendingTasks.Range(func(id string, handle *TaskHandle) bool {
		if len(handle.resultChan) < cap(handle.resultChan) {
			handle.resultChan <- ErrTaskCanceled
		}
		return true
	})

	// Finally, after everything has finished, close the Redis client.
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("streamfleet: failed to close Redis client while closing client: %w", err)
	}

	return nil
}

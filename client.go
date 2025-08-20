package streamfleet

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/redis/go-redis/v9"
)

// TODO Periodically run GC

// TODO On the client side, enforce expiration for tracked tasks.
// This means pending tasks should be failed with an expired error if a reply has not been received.

// ErrClientQueueFull is returned when the client's queue is full.
// This can be returned when enqueuing tasks, or be the logged error when a task is trying to be re-queued after an error.
var ErrClientQueueFull = fmt.Errorf(`streamfleet: client queue is full, tasks are being enqueued too quickly to send to Redis, or Redis is unreachable`)

const clientGroupName = "streamfleet-client"

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
	// Defaults to 25.
	MaxLocalQueueSize int

	// The logger to use.
	// If omitted, uses slog.Default.
	logger *slog.Logger
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

	// The logger to use.
	logger *slog.Logger
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
		opt.MaxLocalQueueSize = 25
	}

	var logger *slog.Logger
	if opt.logger == nil {
		logger = slog.Default()
	} else {
		logger = opt.logger
	}

	c := &Client{
		id:            id,
		isRunning:     true,
		opt:           opt,
		queueToStream: queueToStream,
		client:        client,
		queuedTasks:   make(chan queuedTask, opt.MaxLocalQueueSize),
		pendingTasks:  xsync.NewMap[string, *TaskHandle](),
		logger:        logger,
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
			c.logger.Log(ctx, slog.LevelError, "failed to do client heartbeat",
				"service", "streamfleet.Client",
				"error", err,
			)
			continue
		}
	}
}

// tryQueue tries putting a task on the local queue.
// If the queue is full, returns false. Otherwise, the task was queued and returns true.
func (c *Client) tryQueue(q queuedTask) bool {
	select {
	case c.queuedTasks <- q:
		return true
	default:
		return false
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
	ctx := context.Background()

	for queued := range c.queuedTasks {
		pending, hasP := c.pendingTasks.Load(queued.Task.Id)

		if !c.isRunning {
			if hasP {
				pending.resultChan <- ErrTaskCanceled
			}

			c.logger.Log(ctx, slog.LevelWarn, "client closed, canceling locally queued task",
				"service", "streamfleet.Client",
				"task_id", queued.Task.Id,
				"task_notification_type", TaskNotificationTypeCanceled,
			)
			continue
		}

		retry := func() {
			if !c.tryQueue(queued) {
				// Failed to re-queue because the local queue was full.

				if hasP {
					pending.resultChan <- ErrClientQueueFull
				}

				c.logger.Log(ctx, slog.LevelError, "failed to put task on local queue in Redis, but could not put it back on the local queue because it was full",
					"service", "streamfleet.Client",
					"task_id", queued.Task.Id,
					"error", ErrClientQueueFull,
				)
			}
		}

		if queued.Task.ExpiresTs.After(time.Now()) {
			if hasP {
				pending.resultChan <- ErrTaskExpired
			}

			c.logger.Log(ctx, slog.LevelWarn, "task expired before being sent to Redis",
				"service", "streamfleet.Client",
				"task_id", queued.Task.Id,
				"error", ErrTaskExpired,
			)

			continue
		}

		err := c.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: queued.Stream,
			Values: queued.Task.encode(),

			// TODO Should I use MAXLEN here?
			// What other options are needed?
			// Consult Redis docs.
		})
		if err != nil {
			c.logger.Log(ctx, slog.LevelError, "failed to sent locally queued task to Redis",
				"service", "streamfleet.Client",
				"task_id", queued.Task.Id,
				"error", err,
			)

			retry()
			continue
		}
	}
}

// Listens for pending task notifications and notifies pending task handles.
func (c *Client) notifLoop() {
	ctx := context.Background()

	// TODO Do trimming after receiving new messages (trim IDs below the one received)

	stream := mkRecvStreamKey(c.id)

	for c.isRunning {
		streamResults, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    clientGroupName,
			Streams:  []string{stream},
			Consumer: c.id,
			Block:    0,
			Count:    10,
		}).Result()
		if err != nil {
			// TODO If this is due to the stream or group not existing, recreate it and try again.

			c.logger.Log(ctx, slog.LevelError, "failed to receive task notifications from Redis",
				"service", "streamfleet.Client",
				"error", err,
			)

			// Wait a second before trying again.
			time.Sleep(1 * time.Second)

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
				c.logger.Log(ctx, slog.LevelError, "failed to decode incoming task notification",
					"service", "streamfleet.Client",
					"client_id", c.id,
					"error", err,
				)
				continue
			}

			pending, hasP := c.pendingTasks.LoadAndDelete(notif.TaskId)
			if !hasP {
				// No corresponding local task.
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
				c.logger.Log(ctx, slog.LevelWarn, "received unknown task notification type",
					"service", "streamfleet.Client",
					"client_id", c.id,
					"task_notification_type", notif.Type,
				)
			}
		}

		// Trim stream before the last received message .
		lastMsg := msgs[len(msgs)-1]
		err = c.client.XTrimMinID(ctx, stream, lastMsg.ID).Err()
		if err != nil {
			c.logger.Log(ctx, slog.LevelError, "failed to trim task notifications stream",
				"service", "streamfleet.Client",
				"client_id", c.id,
				"stream", stream,
				"last_msg_id", lastMsg.ID,
			)
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

	// Delete receiver consumer group and stream.
	ctx := context.Background()
	recvKey := mkRecvStreamKey(c.id)
	err := c.client.XGroupDestroy(ctx, recvKey, clientGroupName).Err()
	if err != nil {
		return fmt.Errorf(`streamfleet: failed to delete receiver consumer group %s for stream %s while closing client with ID %s: %w`, clientGroupName, recvKey, c.id, err)
	}
	err = c.client.Del(ctx, recvKey).Err()
	if err != nil {
		return fmt.Errorf(`streamfleet: failed to delete receiver stream %s while closing client with ID %s: %w`, stream, c.id, err)
	}

	// Delete heartbeat entries.
	for queueKey := range c.queueToStream {
		hbKey := mkRecvHeartbeatKey(queueKey)
		err = c.client.ZRem(ctx, hbKey, c.id).Err()
		if err != nil {
			return fmt.Errorf(`streamfleet: failed to remove heartbeat entry for queue key %s while closing client with ID %s: %w`, queueKey, c.id, err)
		}
	}

	// Finally, after everything has finished, close the Redis client.
	if err = c.client.Close(); err != nil {
		return fmt.Errorf("streamfleet: failed to close Redis client while closing client: %w", err)
	}

	return nil
}

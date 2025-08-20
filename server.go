package streamfleet

import (
	"context"
	"errors"
	"fmt"
	"github.com/puzpuzpuz/xsync/v4"
	"log/slog"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// TODO Periodically run GC

// TODO Figure out a good way to trim the stream.
// Right now my strategy is to use XDEL on old entries, but that doesn't work for crashed clients, and there might be more caveats.

// The Redis stream group name used by servers to receive new messages.
const serverGroupName = "streamfleet-server"

// TaskHandler is a function that handles a task.
// Once it returns, the task will be acknowledged as complete if error is nil, otherwise it will be re-queued according to its retry policy.
// If the task is canceled, ctx will be canceled.
type TaskHandler func(ctx context.Context, task *Task) error

// ErrMissingServerUniqueId is returned when a server is instantiated without a unique ID.
var ErrMissingServerUniqueId = fmt.Errorf("streamfleet: server unique ID is missing")

// ErrMissingRedisOpt is returned when a server is instantiated without the necessary options to construct a Redis client.
var ErrMissingRedisOpt = fmt.Errorf("streamfleet: Redis client options are missing")

// ServerOpt are options for Streamfleet servers.
type ServerOpt struct {
	// The unique identifier of the server.
	// This ID should be unique among all servers accepting work from any particular queue.
	// Usually, this should correspond to the name of the logical machine, e.g. its hostname (if unique).
	// Collisions in server unique IDs can result in unexpected behavior, such as work not being distributed evenly among servers.
	//
	// Required.
	ServerUniqueId string

	// The Redis client options to use.
	// See the RedisClientOpt, RedisClusterClientOpt or RedisClusterClientOpt struct for more details.
	//
	// Required.
	RedisOpt ToRedisClient

	// The timeout to use for reconnections to Redis in the event disconnection.
	// If unspecified, there is no timeout and the server will attempt to reconnect forever.
	RetryTimeout time.Duration

	// The number of concurrent handlers to run at once.
	// If unspecified or <1, defaults to 1.
	HandlerConcurrency int

	// The logger to use.
	// If omitted, uses slog.Default.
	logger *slog.Logger
}

// Server is a work queue server (a worker).
// It accepts tasks from the queue and handles them.
type Server struct {
	// Whether the server is running.
	isRunning bool

	// Server options.
	opt ServerOpt

	// Mapping of underlying stream keys to their queue keys.
	streamToQueue map[string]string

	// Mapping of queue keys to their handlers.
	queueHandlers map[string]TaskHandler

	// The underlying Redis client.
	client redis.UniversalClient

	// Pending tasks to be picked up by worker goroutines.
	// Its capacity must match ServerOpt.HandlerConcurrency.
	pendingTasks chan *queuedTask

	// Channel written to for each task processing loop that has exited.
	finishedChan chan struct{}

	// A mapping of IDs of tasks that are currently in progress to their cancellation context.
	// This is used to cancel tasks that are in progress.
	inProgress *xsync.Map[string, context.CancelFunc]

	// The logger to use.
	logger *slog.Logger
}

// NewServer creates a new server instance.
// It does not connect to Redis until Server.Run is called.
func NewServer(opt ServerOpt) (*Server, error) {
	if opt.ServerUniqueId == "" {
		return nil, ErrMissingServerUniqueId
	}
	if opt.RedisOpt == nil {
		return nil, ErrMissingRedisOpt
	}

	client := opt.RedisOpt.ToClient()

	if opt.HandlerConcurrency < 1 {
		opt.HandlerConcurrency = 1
	}

	var logger *slog.Logger
	if opt.logger == nil {
		logger = slog.Default()
	} else {
		logger = opt.logger
	}

	s := &Server{
		isRunning:     false,
		opt:           opt,
		streamToQueue: make(map[string]string),
		queueHandlers: make(map[string]TaskHandler),
		client:        client,
		pendingTasks:  make(chan *queuedTask, opt.HandlerConcurrency),
		finishedChan:  make(chan struct{}, opt.HandlerConcurrency),
		inProgress:    xsync.NewMap[string, context.CancelFunc](),
		logger:        logger,
	}

	for range opt.HandlerConcurrency {
		go s.procLoop()
	}

	return s, nil
}

// Handle sets the handler for the queue with the specified key.
// Once the handler returns, the task will be acknowledged as complete if error is nil, otherwise it will be re-queued according to its retry policy.
// If the server is already running, this function panics.
func (s *Server) Handle(queueKey string, handler TaskHandler) {
	if s.isRunning {
		panic(fmt.Sprintf("streamfleet: server is already running, cannot add handler (queueKey: %s)", queueKey))
	}

	s.queueHandlers[queueKey] = handler
	s.streamToQueue[KeyPrefix+queueKey] = queueKey
}

// sendTaskNotif sends a task notification.
// It logs an error if it fails rather than returning an error.
func (s *Server) sendTaskNotif(clientId string, notif TaskNotification) {
	ctx := context.Background()

	stream := mkRecvStreamKey(clientId)

	for s.isRunning {
		err := s.client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: notif.encode(),
		}).Err()
		if err != nil {
			// Retry if it's due to a network error.
			var opErr *net.OpError
			if errors.As(err, &opErr) {
				s.logger.Log(ctx, slog.LevelWarn, "failed to send task notification due to network error, will retry",
					"service", "streamfleet.Server",
					"server_id", s.opt.ServerUniqueId,
					"task_id", notif.TaskId,
					"error", err,
				)
				time.Sleep(1 * time.Second)
				continue
			}

			s.logger.Log(ctx, slog.LevelError, "failed to send task notification",
				"service", "streamfleet.Server",
				"server_id", s.opt.ServerUniqueId,
				"task_client_id", clientId,
				"task_id", notif.TaskId,
				"task_notification_type", notif.Type,
				"error", err,
			)
			break
		}

		break
	}
}

func (s *Server) retry(ctx context.Context, queued *queuedTask, cause error) {
	go func() {
		task := queued.Task

		time.Sleep(task.RetryDelay)

		task.retries++
		res, err := s.client.XAdd(ctx, &redis.XAddArgs{
			Stream: queued.Stream,
			Values: queued.Task.encode(),
		}).Result()
		if err != nil {
			s.logger.Log(ctx, slog.LevelError, "failed to re-queue task after handler error",
				"service", "streamfleet.Server",
				"server_id", s.opt.ServerUniqueId,
				"task_id", task.Id,
				"stream", queued.Stream,
				"error", err,
			)

			if task.sendNotifications {
				// Send error notification with cause error.
				s.sendTaskNotif(task.ClientId, TaskNotification{
					TaskId: task.Id,
					Type:   TaskNotificationTypeError,
					ErrMsg: cause.Error(),
				})
			}

			// TODO Introduce local holding queue if re-queue on Redis fails.
			// This is because if Redis is down, there may be cascading failures in handlers, and we don't want to burn all retries.
		}

		queued.RedisId = res
	}()
}

func (s *Server) procLoop() {
	defer func() {
		s.finishedChan <- struct{}{}
	}()

	for queued := range s.pendingTasks {
		task := queued.Task

		if !s.isRunning {
			s.logger.Log(context.Background(), slog.LevelDebug, "canceling pending task because server is closed",
				"service", "streamfleet.Server",
				"server_id", s.opt.ServerUniqueId,
				"task_id", task.Id,
			)
			if task.sendNotifications {
				s.sendTaskNotif(task.ClientId, TaskNotification{
					TaskId: task.Id,
					Type:   TaskNotificationTypeCanceled,
				})
			}
			continue
		}

		if queued.Task.ExpiresTs != nil && time.Now().After(*queued.Task.ExpiresTs) {
			if task.sendNotifications {
				s.sendTaskNotif(queued.Task.ClientId, TaskNotification{
					TaskId: queued.Task.Id,
					Type:   TaskNotificationTypeExpired,
				})
			}
			continue
		}

		handler, hasHandler := s.queueHandlers[queued.Queue]
		if !hasHandler {
			s.logger.Log(context.Background(), slog.LevelError, "received message from queue without a registered, this is a bug in streamfleet",
				"service", "streamfleet.Server",
				"server_id", s.opt.ServerUniqueId,
				"queue", queued.Queue,
			)
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())
		cleanup := func() {
			cancel()
			s.inProgress.Delete(queued.Task.Id)
		}

		s.inProgress.Store(queued.Task.Id, cancel)

		handlerDone := make(chan struct{})

		// Make sure the task is periodically claimed by this worker while it is in progress.
		// This prevents other workers from thinking this server is dead and claiming the task for themselves.
		go func() {
			timer := time.NewTimer(TaskUpdatePendingInterval)
			for {
				select {
				case <-timer.C:
					err := s.client.XClaim(ctx, &redis.XClaimArgs{
						Stream:   queued.Stream,
						Group:    serverGroupName,
						Consumer: s.opt.ServerUniqueId,
						Messages: []string{queued.RedisId},
					}).Err()
					if err != nil && !errors.Is(err, context.Canceled) {
						s.logger.Log(ctx, slog.LevelError, "failed to keep task in server custody with XCLAIM",
							"service", "streamfleet.Server",
							"server_id", s.opt.ServerUniqueId,
							"task_id", queued.Task.Id,
							"error", err,
						)
					}
				case <-handlerDone:
					timer.Stop()
					return
				}
			}
		}()

		// Actually run the handler.
		err := handler(ctx, queued.Task)
		handlerDone <- struct{}{}
		if err != nil {
			if queued.Task.MaxRetries == 0 || queued.Task.retries < queued.Task.MaxRetries {
				s.logger.Log(ctx, slog.LevelError, "task handler returned error, will retry",
					"service", "streamfleet.Server",
					"server_id", s.opt.ServerUniqueId,
					"task_id", queued.Task.Id,
					"error", err,
					"retries", queued.Task.retries,
					"max_retries", queued.Task.MaxRetries,
					"will_retry", true,
				)

				// Try to re-queue.
				s.retry(ctx, queued, err)
			} else {
				s.logger.Log(ctx, slog.LevelError, "task handler returned error, will not retry",
					"service", "streamfleet.Server",
					"server_id", s.opt.ServerUniqueId,
					"task_id", queued.Task.Id,
					"error", err,
					"retries", queued.Task.retries,
					"max_retries", queued.Task.MaxRetries,
					"will_retry", false,
				)

				if task.sendNotifications {
					// Since there are no retries left, fail task with error.
					s.sendTaskNotif(task.ClientId, TaskNotification{
						TaskId: task.Id,
						Type:   TaskNotificationTypeError,
						ErrMsg: err.Error(),
					})
				}
			}
		} else if task.sendNotifications {
			s.sendTaskNotif(task.ClientId, TaskNotification{
				TaskId: task.Id,
				Type:   TaskNotificationTypeCompleted,
			})
		}

		skipIter := false
		for s.isRunning {
			// Regardless of whether the handler exited with success or failure, acknowledge the message.
			// If it failed, it should have already had a duplicate added by this point.
			err = s.client.XAck(context.Background(), queued.Stream, serverGroupName, queued.RedisId).Err()
			if err != nil {
				// Retry if it's due to a network error.
				var opErr *net.OpError
				if errors.As(err, &opErr) {
					s.logger.Log(ctx, slog.LevelWarn, "failed to acknowledge handled task due to network error, will retry",
						"service", "streamfleet.Server",
						"server_id", s.opt.ServerUniqueId,
						"task_id", queued.Task.Id,
						"error", err,
					)
					time.Sleep(1 * time.Second)
					continue
				}

				s.logger.Log(ctx, slog.LevelError, "failed to acknowledge handled task",
					"service", "streamfleet.Server",
					"server_id", s.opt.ServerUniqueId,
					"task_id", queued.Task.Id,
					"error", err,
				)

				cleanup()
				skipIter = true
				break
			}

			break
		}
		if skipIter {
			continue
		}

		// TODO Replace delete with trim?
		err = s.client.XDel(context.Background(), queued.Stream, queued.RedisId).Err()
		if err != nil {
			s.logger.Log(ctx, slog.LevelError, "failed to delete handled task",
				"service", "streamfleet.Server",
				"server_id", s.opt.ServerUniqueId,
				"task_id", queued.Task.Id,
				"error", err,
			)

			cleanup()
			continue
		}

		cleanup()
	}
}

// Main stream receiver loop.
// Only errors if there is a failure reading from Redis or decoding tasks.
// If this function returns, the Redis client should be considered as closed.
func (s *Server) recvLoop() error {
	ctx := context.Background()

	for s.isRunning {
		streams := make([]string, 0, len(s.streamToQueue)*2)
		for stream := range s.streamToQueue {
			streams = append(streams, stream, ">")
		}

		streamResults, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    serverGroupName,
			Streams:  streams,
			Consumer: s.opt.ServerUniqueId,
			Block:    0,
			Count:    int64(s.opt.HandlerConcurrency),
		}).Result()
		if err != nil {
			if !s.isRunning || errors.Is(err, redis.ErrClosed) {
				break
			}

			// TODO Figure out if error can be ignored
			return fmt.Errorf("streamfleet: server failed to read from Redis stream %s: %w", streamResults, err)
		}

		// For each stream, decode and queue all received tasks.
		for _, result := range streamResults {
			queue, hasQ := s.streamToQueue[result.Stream]
			if !hasQ {
				s.logger.Log(ctx, slog.LevelError, "received message from unknown stream, this is a bug in streamfleet",
					"service", "streamfleet.Server",
					"server_id", s.opt.ServerUniqueId,
					"stream", result.Stream,
				)
				continue
			}

			// Queue each task from the stream.
			for _, msg := range result.Messages {
				var task *Task
				task, err = decodeTask(msg.Values)
				if err != nil {
					s.logger.Log(ctx, slog.LevelError, "failed to decode incoming task",
						"service", "streamfleet.Server",
						"server_id", s.opt.ServerUniqueId,
						"task_id", msg.ID,
						"queue", queue,
						"error", err,
					)
					continue
				}

				s.pendingTasks <- &queuedTask{
					Stream:  result.Stream,
					Queue:   queue,
					Task:    task,
					RedisId: msg.ID,
				}
			}
		}
	}

	return nil
}

// Run runs the server.
// It will only return if the server's initial connection fails, or retrying the connection times out.
// Note that handlers are not guaranteed to run on the same goroutine as this call.
func (s *Server) Run() error {
	ctx := context.Background()

	if len(s.streamToQueue) == 0 {
		return ErrNoQueues
	}

	for stream := range s.streamToQueue {
		err := s.client.XGroupCreateMkStream(ctx, stream, serverGroupName, "0").Err()
		if err != nil {
			// TODO Ignore BUSYGROUP errors
			return fmt.Errorf("streamfleet: failed to create stream %s or group %s for the stream: %w", stream, serverGroupName, err)
		}
	}

	s.isRunning = true

	for s.isRunning {
		err := s.recvLoop()
		if err != nil {
			return fmt.Errorf("streamfleet: server failed to run: %w", err)
		}
	}

	return nil
}

// Close stops the server.
// Will cancel pending tasks and wait for processing loops to finish.
// The server must not be used after calling Close.
func (s *Server) Close() error {
	s.isRunning = false

	close(s.pendingTasks)

	// Cancel all in-progress tasks.
	s.inProgress.Range(func(id string, cancel context.CancelFunc) bool {
		cancel()
		s.inProgress.Delete(id)
		return true
	})

	// Wait for all task processing loops to finish.
	for range s.opt.HandlerConcurrency {
		<-s.finishedChan
	}

	// Finally, after everything has finished, close the Redis client.
	if err := s.client.Close(); err != nil {
		return fmt.Errorf("streamfleet: failed to close Redis client while closing server: %w", err)
	}

	return nil
}

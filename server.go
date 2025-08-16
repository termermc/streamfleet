package streamfleet

import (
	"context"
	"errors"
	"fmt"
	"github.com/puzpuzpuz/xsync/v4"
	"time"

	"github.com/redis/go-redis/v9"
)

// TODO Figure out a good way to trim the stream.
// Right now my strategy is to use XDEL on old entries, but that doesn't work for crashed clients, and there might be more caveats.

type queuedTask struct {
	Id     string
	Stream string
	Queue  string
	Task   *Task
}

// The Redis stream group name used by servers to receive new messages.
const serverGroupName = "streamfleet-server"

// TaskHandler is a function that handles a task.
// Once it returns, the task will be acknowledged as complete if error is nil, otherwise it will be re-queued according to its retry policy.
// If the task is canceled, ctx will be canceled.
type TaskHandler func(ctx context.Context, task *Task) error

// ErrMissingServerUniqueId is returned when a server is instantiated without a unique ID.
var ErrMissingServerUniqueId = fmt.Errorf("streamfleet: server unique ID is missing")

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

	// TODO Pluggable logger
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
	pendingTasks chan queuedTask

	// Channel written to for each task processing loop that has exited.
	finishedChan chan struct{}

	// A mapping of IDs of tasks that are currently in progress to their cancellation context.
	// This is used to cancel tasks that are in progress.
	inProgress *xsync.Map[string, context.CancelFunc]
}

// NewServer creates a new server instance.
// It does not connect to Redis until Server.Run is called.
func NewServer(opt ServerOpt) *Server {
	client := opt.RedisOpt.ToClient()

	if opt.HandlerConcurrency < 1 {
		opt.HandlerConcurrency = 1
	}

	s := &Server{
		isRunning:     false,
		opt:           opt,
		streamToQueue: make(map[string]string),
		queueHandlers: make(map[string]TaskHandler),
		client:        client,
		pendingTasks:  make(chan queuedTask, opt.HandlerConcurrency),
		finishedChan:  make(chan struct{}, opt.HandlerConcurrency),
		inProgress:    xsync.NewMap[string, context.CancelFunc](),
	}

	for range opt.HandlerConcurrency {
		go s.procLoop()
	}

	return s
}

// Handle sets the handler for the queue with the specified key.
// Once the handler returns, the task will be acknowledged as complete if error is nil, otherwise it will be re-queued according to its retry policy.
// If the server is already running, this function panics.
func (s *Server) Handle(queueKey string, handler TaskHandler) {
	if s.isRunning {
		panic(fmt.Sprintf("streamfleet: server is already running, cannot add handler (queueKey: %s)", queueKey))
	}

	s.queueHandlers[queueKey] = handler
	s.streamToQueue[QueueStreamPrefix+queueKey] = queueKey
}

func (s *Server) procLoop() {
	defer func() {
		s.finishedChan <- struct{}{}
	}()

	for queued := range s.pendingTasks {
		if !s.isRunning {
			// TODO Mark task as canceled and log
			continue
		}

		handler, hasHandler := s.queueHandlers[queued.Queue]
		if !hasHandler {
			// TODO Log error about receiving message for unknown queue, stating that it is a bug in the library
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())
		cleanup := func() {
			cancel()
			s.inProgress.Delete(queued.Id)
		}

		s.inProgress.Store(queued.Id, cancel)

		handlerDone := make(chan struct{})

		// Make sure the task is periodically claimed by this worker while it is in progress.
		// This prevents other workers from thinking this server is dead and claiming the task for themselves.
		go func() {
			ticker := time.NewTicker(TaskUpdatePendingInterval)
			for {
				select {
				case <-ticker.C:
					err := s.client.XClaim(ctx, &redis.XClaimArgs{
						Stream:   queued.Stream,
						Group:    serverGroupName,
						Consumer: s.opt.ServerUniqueId,
						Messages: []string{queued.Id},
					}).Err()
					if err != nil && !errors.Is(err, context.Canceled) {
						// TODO Log error about failing to XCLAIM task
					}
				case <-handlerDone:
					return
				}
			}
		}()

		// Actually run the handler.
		err := handler(ctx, queued.Task)
		handlerDone <- struct{}{}
		if err != nil {
			// TODO Log error about failing to handle task
			// TODO Regardless of whether this was due to cancelation, abide by the retry policy.

			if queued.Task.MaxRetries == 0 || queued.Task.retries < queued.Task.MaxRetries {
				// TODO Re-queue
			} else {
				// TODO Log that task failed too many times
			}
		}

		// Regardless of whether the handler exited with success or failure, acknowledge the message.
		// If it failed, it should have already had a duplicate added by this point.
		err = s.client.XAck(context.Background(), queued.Stream, serverGroupName, queued.Id).Err()
		if err != nil {
			// TODO Log error about failing to acknowledge task.

			cleanup()
			continue
		}
		err = s.client.XDel(context.Background(), queued.Stream, queued.Id).Err()
		if err != nil {
			// TODO Log error about failing to delete task.

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
		streams := make([]string, 0, len(s.streamToQueue))
		for stream := range s.streamToQueue {
			streams = append(streams, stream)
		}

		streamResults, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    serverGroupName,
			Streams:  streams,
			Consumer: s.opt.ServerUniqueId,
			Block:    0,
			Count:    int64(s.opt.HandlerConcurrency),
		}).Result()
		if err != nil {
			// TODO Figure out if error can be ignored
			return fmt.Errorf("streamfleet: server failed to read from Redis: %w", err)
		}

		// For each stream, decode and queue all received tasks.
		for _, result := range streamResults {
			queue, hasQ := s.streamToQueue[result.Stream]
			if !hasQ {
				// TODO Log error about receiving message from unknown stream, stating that it is a bug in the library
				continue
			}

			// Queue each task from the stream.
			for _, msg := range result.Messages {
				var task *Task
				task, err = decodeTask(msg.Values)
				if err != nil {
					// TODO Log error about failing to decode task
					continue
				}

				s.pendingTasks <- queuedTask{
					Id:     msg.ID,
					Stream: result.Stream,
					Queue:  queue,
					Task:   task,
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

	for stream := range s.streamToQueue {
		err := s.client.XGroupCreateMkStream(ctx, stream, serverGroupName, "0").Err()
		if err != nil {
			// TODO Ignore BUSYGROUP errors
			return fmt.Errorf("streamfleet: failed to create stream %s or group %s for the stream: %w", stream, serverGroupName, err)
		}
	}

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

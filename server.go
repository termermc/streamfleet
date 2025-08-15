package streamfleet

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TODO Figure out a good way to trim the stream

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
	pendingTasks chan *Task
}

// NewServer creates a new server instance.
// It does not connect to Redis until Server.Run is called.
func NewServer(opt ServerOpt) *Server {
	client := opt.RedisOpt.ToClient()

	if opt.HandlerConcurrency < 1 {
		opt.HandlerConcurrency = 1
	}

	return &Server{
		isRunning:     false,
		opt:           opt,
		streamToQueue: make(map[string]string),
		queueHandlers: make(map[string]TaskHandler),
		client:        client,
		pendingTasks:  make(chan *Task, opt.HandlerConcurrency),
	}
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

// Main stream receiver loop.
// Only errors if there is a failure reading from Redis.
// If this function returns, the Redis client should be considered as closed.
func (s *Server) recvLoop() error {
	ctx := context.Background()

	streams := make([]string, 0, len(s.streamToQueue))
	for stream := range s.streamToQueue {
		streams = append(streams, stream)
	}

	// TODO Read from streams
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

	for _, result := range streamResults {
		queue, hasQ := s.streamToQueue[result.Stream]
		if !hasQ {
			// TODO Log error about receiving message from unknown stream, stating that it is a bug in the library
			continue
		}

		handler, hasH := s.queueHandlers[queue]
		if !hasH {
			// TODO Log error about receiving message from queue without a handler, stating that it is a bug in the library
			continue
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

	// TODO Run recvLoop, repeat (taking into account reconnect timeout).

	return nil
}

// Close stops the server.
// Pending tasks will be canceled.
func (s *Server) Close() error {
	s.isRunning = false
	if err := s.client.Close(); err != nil {
		return fmt.Errorf("streamfleet: failed to close Redis client while closing server: %w", err)
	}

	// TODO Signal to pending tasks to cancel

	return nil
}

package streamfleet

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

// TaskHandler is a function that handles a task.
// Once it returns, the task will be acknowledged as complete if error is nil, otherwise it will be re-queued according to its retry policy.
// If the task is canceled, ctx will be canceled.
type TaskHandler func(ctx context.Context, task *Task) error

// ServerOpt are options for Streamfleet servers.
type ServerOpt[T redis.Client | redis.ClusterClient] struct {
	// The Redis client options to use.
	// See the RedisClientOpt, RedisClusterClientOpt or RedisClusterClientOpt struct for more details.
	// Required.
	RedisOpt ToRedisClient[T]

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
type Server[TRedis redis.Client | redis.ClusterClient] struct {
	// Whether the server is running.
	isRunning bool

	// The timeout reconnections to the Redis server.
	retryTimeout time.Duration

	// Mapping of underlying stream keys to their queue keys.
	streamToQueue map[string]string

	// Mapping of queue keys to their handlers.
	queueHandlers map[string]TaskHandler

	// The underlying Redis client.
	client *TRedis
}

// NewServer creates a new server instance.
// It does not connect to Redis until Server.Run is called.
func NewServer[TRedis redis.Client | redis.ClusterClient](redisOpt ToRedisClient[TRedis], retryTimeout time.Duration) *Server[TRedis] {
	client := redisOpt.ToClient()

	return &Server[TRedis]{
		isRunning:     false,
		retryTimeout:  retryTimeout,
		streamToQueue: make(map[string]string),
		queueHandlers: make(map[string]TaskHandler),
		client:        client,
	}
}

// Handle sets the handler for the queue with the specified key.
// Once the handler returns, the task will be acknowledged as complete if error is nil, otherwise it will be re-queued according to its retry policy.
// If the server is already running, this function panics.
func (s *Server[_]) Handle(queueKey string, handler TaskHandler) {
	if s.isRunning {
		panic(fmt.Sprintf("streamfleet: server is already running, cannot add handler (queueKey: %s)", queueKey))
	}

	s.queueHandlers[queueKey] = handler
}

// Main stream receiver loop.
// Only errors if there is a failure reading from Redis.
// If this function returns, the Redis client should be considered as closed.
func (s *Server[_]) recvLoop() error {
	// TODO Read from streams

	return nil
}

// Run runs the server.
// It will only return if the server's initial connection fails, or retrying the connection times out.
func (s *Server[_]) Run() error {
	// TODO Connect, handle errors, run recvLoop, repeat (taking into account reconnect timeout).
	return nil
}

// Close stops the server.
// Pending tasks will be canceled.
func (s *Server[_]) Close() error {
	// TODO
	return nil
}

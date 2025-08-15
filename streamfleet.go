package streamfleet

import "time"

// TaskMaxPendingTime is the maximum time a task can sit in a worker's pending list before it is reclaimable by other workers.
// Tasks pending time is updated by healthy clients at the interval specified by TaskUpdatePendingInterval.
// This duration will always be higher than TaskUpdatePendingInterval to ensure healthy tasks aren't being reclaimed.
const TaskMaxPendingTime = 1 * time.Minute

// TaskUpdatePendingInterval is the interval at which the pending time of a task is updated.
const TaskUpdatePendingInterval = 10 * time.Second

// QueuePrefix is the prefix use for all Streamfleet work queues Redis stream keys.
// The underlying stream key for a queue will be QueuePrefix + the queue's key.
const QueuePrefix = "streamfleet:"

// Task is a task to be processed by a worker.
// It is sent to the queue, and then processed by a worker.
// The only required field is Data.
type Task struct {
	// The task's underlying data.
	// Required, but technically can be nil/empty.
	Data []byte

	// The task's maximum retry count.
	// If a task is retried more than this many times, it will be removed from the queue, and optionally send a canceled signal.
	// If 0, the task will be retried until success.
	MaxRetries int

	// The timestamp when the task expires.
	// If a worker receives the task past this timestamp, it will discard it, and optionally send a canceled signal.
	// If not, never expires.
	ExpiresTs *time.Time

	// The duration to delay retrying the task.
	RetryDelay time.Duration

	// Whether status notifications for this task should be sent by the worker handling it.
	// This is set when the task is enqueued based on which enqueue method is used.
	sendNotifications bool

	// The current number of retries.
	retries int
}

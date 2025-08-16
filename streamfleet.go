package streamfleet

import (
	"errors"
	"time"
)

// The current encoding version used for encoding tasks in Redis stream messages.
const curTaskEncVer = 1

// TaskMaxPendingTime is the maximum time a task can sit in a worker's pending list before it is reclaimable by other workers.
// Tasks pending time is updated by healthy clients at the interval specified by TaskUpdatePendingInterval.
// This duration will always be higher than TaskUpdatePendingInterval to ensure healthy tasks aren't being reclaimed.
const TaskMaxPendingTime = 1 * time.Minute

// TaskUpdatePendingInterval is the interval at which the pending time of a task is updated.
const TaskUpdatePendingInterval = 10 * time.Second

// QueueStreamPrefix is the prefix use for all Streamfleet work queues Redis stream keys.
// The underlying stream key for a queue will be QueueStreamPrefix + the queue's key.
const QueueStreamPrefix = "streamfleet:"

// Task is a task to be processed by a worker.
// It is sent to the queue, and then processed by a worker.
// The only required field is Data.
// Do not construct directly; use NewTask.
type Task struct {
	// TODO Include a permanent random ID here.
	// This is necessary because the underlying Redis message IDs might change when tasks are re-queued because of failures.
	// We still need a stable ID to broadcast updates for.

	// The message's unique ID.
	// TODO UUIDv7?
	Id string

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

func (t *Task) encode() map[string]any {
	var expTs int64
	if t.ExpiresTs != nil {
		expTs = t.ExpiresTs.UnixMilli()
	}

	return map[string]any{
		"enc_version": curTaskEncVer,
		"data":        t.Data,
		"max_retries": t.MaxRetries,
		"expires_ts":  expTs,
		"retry_delay": t.RetryDelay.Milliseconds(),
		"send_notif":  t.sendNotifications,
		"retries":     t.retries,
	}
}

// ErrMissingOrMalformedTaskVersion is returned when the version field of an encoded task from a Redis stream is missing or malformed.
var ErrMissingOrMalformedTaskVersion = errors.New("streamfleet: missing or malformed enc_version in encoded task")

// ErrUnsupportedTaskVersion is returned when the version field of an encoded task from a Redis stream is unsupported.
var ErrUnsupportedTaskVersion = errors.New("streamfleet: unsupported enc_version in encoded task, this node may be running an outdated version of Streamfleet")

// decodeTask decodes a task from a raw Redis stream message.
// Returns ErrMissingOrMalformedTaskVersion if the version field is missing or malformed.
// Returns ErrUnsupportedTaskVersion if the version field is unsupported.
func decodeTask(msg map[string]any) (*Task, error) {
	verAny, ok := msg["enc_version"]
	if !ok || verAny == nil {
		return nil, ErrMissingOrMalformedTaskVersion
	}
	verInt, ok := verAny.(int)
	if !ok {
		return nil, ErrMissingOrMalformedTaskVersion
	}

	switch verInt {
	case 1:
		fieldData := msg["data"].([]byte)
		fieldMaxRetries := msg["max_retries"].(int)
		fieldExpiresTs := msg["expires_ts"].(int64)
		fieldRetryDelay := msg["retry_delay"].(int64)
		fieldSendNotif := msg["send_notif"].(bool)
		fieldRetries := msg["retries"].(int)

		var expiresTs *time.Time
		if fieldExpiresTs != 0 {
			ts := time.UnixMilli(fieldExpiresTs)
			expiresTs = &ts
		}

		return &Task{
			Data:              fieldData,
			MaxRetries:        fieldMaxRetries,
			ExpiresTs:         expiresTs,
			RetryDelay:        time.Duration(fieldRetryDelay) * time.Millisecond,
			sendNotifications: fieldSendNotif,
			retries:           fieldRetries,
		}, nil
	default:
		return nil, ErrUnsupportedTaskVersion
	}
}

package streamfleet

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// The current encoding version used for encoding tasks in Redis stream messages.
const curTaskEncVer = "1"

// The current encoding version used for encoding task notifications in Redis stream messages.
const curTaskNotifEncVer = "1"

// TaskMaxPendingTime is the maximum time a task can sit in a worker's pending list before it is reclaimable by other workers.
// Tasks pending time is updated by healthy clients at the interval specified by TaskUpdatePendingInterval.
// This duration will always be higher than TaskUpdatePendingInterval to ensure healthy tasks aren't being reclaimed.
const TaskMaxPendingTime = 1 * time.Minute

// TaskUpdatePendingInterval is the interval at which the pending time of a task is updated.
const TaskUpdatePendingInterval = 10 * time.Second

// KeyPrefix is the prefix used by Streamfleet for all Redis keys, including work queue streams.
// The underlying stream key for a queue will be KeyPrefix + the queue's key.
const KeyPrefix = "streamfleet:"

// ErrNoQueues is returned when running a server or instantiating a client without specifying any queue keys.
var ErrNoQueues = fmt.Errorf("streamfleet: tried to run a server or instantiate a client that had no configured queue keys")

// ErrTaskCanceled is returned when a task has been canceled.
var ErrTaskCanceled = fmt.Errorf("streamfleet: task canceled")

// ErrTaskExpired is returned when a task could not be handled before its expiration time.
var ErrTaskExpired = fmt.Errorf("streamfleet: task expired")

// Task is a task to be processed by a worker.
// It is sent to the queue, and then processed by a worker.
// The only required field is Data.
// Do not construct directly, use client enqueue methods.
type Task struct {
	// TODO Include a permanent random ID here.
	// This is necessary because the underlying Redis message IDs might change when tasks are re-queued because of failures.
	// We still need a stable ID to broadcast updates for.
	// TODO Update server to make use of this ID instead of the stream message ID.

	// The message's unique ID.
	Id string

	// The ID of the client where the task originated.
	ClientId string

	// The task's underlying data.
	// Required, but technically can be nil/empty.
	Data string

	// The task's maximum retry count.
	// If a task is retried more than this many times, it will be removed from the queue, and optionally send a canceled signal.
	// If 0, the task will be retried until success.
	MaxRetries int

	// The timestamp when the task expires.
	// If a worker receives the task past this timestamp, it will discard it, and optionally send a canceled signal.
	// If not, never expires.
	// Precision below 1 second is not guaranteed to be enforced immediately by clients; it may take up to 1 second for expirations to be notified.
	ExpiresTs *time.Time

	// The duration to delay retrying the task.
	RetryDelay time.Duration

	// Whether status notifications for this task should be sent by the worker handling it.
	// This is set when the task is enqueued based on which enqueue method is used.
	sendNotifications bool

	// The current number of retries.
	retries int
}

// TaskOpt are options for creating a task.
// All fields are optional.
type TaskOpt struct {
	// The maximum number of retries for this task.
	// If omitted or 0, the task will be retried until success or expiration.
	MaxRetries int

	// The timestamp when the task expires.
	// If a worker receives the task past this timestamp, it will discard it.
	// If omitted, the task will never expire.
	// TODO Make this not a pointer for better library ergonomics
	ExpiresTs *time.Time

	// The duration to delay retrying the task.
	// If omitted, there is no delay.
	RetryDelay time.Duration
}

func newTask(data string, clientId string, sendNotif bool, opt TaskOpt) *Task {
	return &Task{
		Id:                MustUuidV7(),
		ClientId:          clientId,
		Data:              data,
		MaxRetries:        opt.MaxRetries,
		ExpiresTs:         opt.ExpiresTs,
		RetryDelay:        opt.RetryDelay,
		sendNotifications: sendNotif,
		retries:           0,
	}
}

func (t *Task) encode() map[string]any {
	var expTs int64
	if t.ExpiresTs != nil {
		expTs = t.ExpiresTs.UnixMilli()
	}

	return map[string]any{
		"enc_version": curTaskEncVer,
		"id":          t.Id,
		"client_id":   t.ClientId,
		"data":        t.Data,
		"max_retries": t.MaxRetries,
		"expires_ts":  expTs,
		"retry_delay": t.RetryDelay.Milliseconds(),
		"send_notif":  t.sendNotifications,
		"retries":     t.retries,
	}
}

// ErrMissingOrMalformedEncodingVersion is returned when the version field of an encoded message from a Redis stream is missing or malformed.
var ErrMissingOrMalformedEncodingVersion = errors.New("streamfleet: missing or malformed enc_version in encoded message")

// ErrUnsupportedEncodingVersion is returned when the version field of an encoded message from a Redis stream is unsupported.
var ErrUnsupportedEncodingVersion = errors.New("streamfleet: unsupported enc_version in encoded message, this node may be running an outdated version of Streamfleet")

// decodeTask decodes a task from a raw Redis stream message.
// Returns ErrMissingOrMalformedEncodingVersion if the version field is missing or malformed.
// Returns ErrUnsupportedEncodingVersion if the version field is unsupported.
func decodeTask(msg map[string]any) (*Task, error) {
	verAny, ok := msg["enc_version"]
	if !ok || verAny == nil {
		return nil, ErrMissingOrMalformedEncodingVersion
	}
	verInt, err := strconv.ParseInt(verAny.(string), 10, 64)
	if err != nil {
		return nil, ErrMissingOrMalformedEncodingVersion
	}

	switch verInt {
	case 1:
		fieldId := msg["id"].(string)
		fieldClientId := msg["client_id"].(string)
		fieldData := msg["data"].(string)
		fieldMaxRetriesStr := msg["max_retries"].(string)
		fieldExpiresTsStr := msg["expires_ts"].(string)
		fieldRetryDelayStr := msg["retry_delay"].(string)
		fieldSendNotif := msg["send_notif"] != "0"
		fieldRetriesStr := msg["retries"].(string)

		fieldMaxRetries, _ := strconv.ParseInt(fieldMaxRetriesStr, 10, 64)
		fieldExpiresTs, _ := strconv.ParseInt(fieldExpiresTsStr, 10, 64)
		fieldRetryDelay, _ := strconv.ParseInt(fieldRetryDelayStr, 10, 64)
		fieldRetries, _ := strconv.ParseInt(fieldRetriesStr, 10, 64)

		var expiresTs *time.Time
		if fieldExpiresTs != 0 {
			ts := time.UnixMilli(fieldExpiresTs)
			expiresTs = &ts
		}

		return &Task{
			Id:                fieldId,
			ClientId:          fieldClientId,
			Data:              fieldData,
			MaxRetries:        int(fieldMaxRetries),
			ExpiresTs:         expiresTs,
			RetryDelay:        time.Duration(fieldRetryDelay) * time.Millisecond,
			sendNotifications: fieldSendNotif,
			retries:           int(fieldRetries),
		}, nil
	default:
		return nil, ErrUnsupportedEncodingVersion
	}
}

// TaskHandle is a handle to a task that was enqueued by a client.
// The task may or may not have been completed.
type TaskHandle struct {
	// The task's unique ID.
	Id string

	expTs *time.Time

	hasResult bool
	result    error

	// A channel written to and closed when the task is completed.
	// If the task completed successfully, the value will be nil.
	// If the task failed (or was canceled), the value will be an error.
	resultChan chan error
}

// Wait waits for the task to complete and returns the result.
// Subsequent calls will return the same result.
// If the task was canceled, the error will be ErrTaskCanceled.
// If the task expired before being processed, the error will be ErrTaskExpired.
//
// Important: Do not call this method concurrently, otherwise it may panic.
func (t *TaskHandle) Wait() error {
	if t.hasResult {
		return t.result
	}

	err := <-t.resultChan
	t.hasResult = true
	t.result = err

	return err
}

type queuedTask struct {
	Stream  string
	Queue   string
	Task    *Task
	RedisId string
}

func mkRecvHeartbeatKey(queueKey string) string {
	return KeyPrefix + queueKey + ":receivers"
}
func mkRecvStreamKey(id string) string {
	return KeyPrefix + "receiver:" + id
}

// The timeout for client receiver stream heartbeats.
// Receiver streams who haven't had a heartbeat for this long will be cleaned up.
const receiverStreamHeartbeatTimeout = 10 * time.Minute

// The interval at which clients emit heartbeats.
const receiverStreamHeartbeatInterval = 10 * time.Second

// runReceiverStreamGc runs the receiver stream garbage collector.
// It finds receiver streams who haven't had a heartbeat in the last receiverStreamHeartbeatTimeout and deletes them.
func runReceiverStreamGc(ctx context.Context, client redis.UniversalClient, queueKeys []string) error {
	for _, queue := range queueKeys {
		setKey := mkRecvHeartbeatKey(queue)

		// Find receiver IDs whose last heartbeat was too long ago.
		timeoutCutoff := time.Now().Add(-receiverStreamHeartbeatTimeout)
		ids, err := client.ZRangeByScore(ctx, setKey, &redis.ZRangeBy{
			Max: strconv.FormatInt(timeoutCutoff.UnixMilli(), 10),
		}).Result()
		if err != nil {
			return fmt.Errorf(`streamfleet: gc: failed to get receiver stream IDs whose heartbeats timed out: %w`, err)
		}

		if len(ids) == 0 {
			continue
		}

		// Remove receiver streams.
		for _, id := range ids {
			recvKey := mkRecvStreamKey(id)

			// Get groups and delete them.
			var groups []redis.XInfoGroup
			groups, err = client.XInfoGroups(ctx, recvKey).Result()
			if err != nil {
				return fmt.Errorf(`streamfleet: gc: failed to get groups on stream with key "%s" that is pending deletion: %w`, recvKey, err)
			}

			for _, group := range groups {
				err = client.XGroupDestroy(ctx, recvKey, group.Name).Err()
				if err != nil {
					return fmt.Errorf(`streamfleet: gc: failed to delete group "%s" on stream with key "%s" that is pending deletion: %w`, group.Name, recvKey, err)
				}
			}

			err = client.Del(ctx, recvKey, id).Err()
			if err != nil {
				return fmt.Errorf(`streamfleet: gc: failed to delete receiver stream with key "%s": %w`, recvKey, err)
			}
		}

		err = client.ZRem(ctx, setKey, ids).Err()
		if err != nil {
			return fmt.Errorf(`streamfleet: gc: failed to remove items from receiver stream heartbeat set with key "%s": %w`, setKey, err)
		}
	}

	return nil
}

type TaskNotificationType string

const (
	// TaskNotificationTypeCompleted is sent when a task is completed successfully.
	TaskNotificationTypeCompleted TaskNotificationType = "completed"

	// TaskNotificationTypeCanceled is sent when a task is canceled.
	TaskNotificationTypeCanceled TaskNotificationType = "canceled"

	// TaskNotificationTypeExpired is sent when a task expires.
	TaskNotificationTypeExpired TaskNotificationType = "expired"

	// TaskNotificationTypeError is sent when a task fails due to an error.
	// The notification should include the underlying error message.
	TaskNotificationTypeError TaskNotificationType = "error"
)

// TaskNotification is a notification sent about a task's status.
type TaskNotification struct {
	// The task ID.
	TaskId string

	// The task type.
	Type TaskNotificationType

	// The error message, if Type is TaskNotificationTypeError.
	// Otherwise, empty.
	ErrMsg string
}

func (t *TaskNotification) encode() map[string]any {
	return map[string]any{
		"enc_version": curTaskNotifEncVer,
		"task_id":     t.TaskId,
		"type":        string(t.Type),
		"err_msg":     t.ErrMsg,
	}
}

// decodeTaskNotification decodes a task notification from a raw Redis stream message.
// Returns ErrMissingOrMalformedEncodingVersion if the version field is missing or malformed.
// Returns ErrUnsupportedEncodingVersion if the version field is unsupported.
func decodeTaskNotification(msg map[string]any) (*TaskNotification, error) {
	verAny, ok := msg["enc_version"]
	if !ok || verAny == nil {
		return nil, ErrMissingOrMalformedEncodingVersion
	}
	verInt, err := strconv.ParseInt(verAny.(string), 10, 64)
	if err != nil {
		return nil, ErrMissingOrMalformedEncodingVersion
	}

	switch verInt {
	case 1:
		fieldTaskId := msg["task_id"].(string)
		fieldType := msg["type"].(string)
		fieldErrMsg := msg["err_msg"].(string)

		return &TaskNotification{
			TaskId: fieldTaskId,
			Type:   TaskNotificationType(fieldType),
			ErrMsg: fieldErrMsg,
		}, nil
	default:
		return nil, ErrUnsupportedEncodingVersion
	}
}

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/termermc/streamfleet"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const TestQueue1 = "test-queue-1"

func prepareRedis(ctx context.Context) (cont testcontainers.Container, addr string, err error) {
	port := strconv.FormatInt(6380+rand.Int64N(25), 10)

	req := testcontainers.ContainerRequest{
		Image:        "docker.io/redis:8.2.1-alpine3.22",
		ExposedPorts: []string{port + ":6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	cont, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to start Redis container: %w", err)
	}

	return cont, "127.0.0.1:" + port, nil
}

type dependencies struct {
	Container testcontainers.Container
	RedisAddr string
	Client    *streamfleet.Client
	Server    *streamfleet.Server
}

func (d *dependencies) Close() {
	_ = d.Client.Close()
	_ = d.Server.Close()

	// Give time for server and client to close.
	time.Sleep(1 * time.Second)
	_ = d.Container.Stop(context.Background(), nil)
}

func mkDeps(opts ...any) (*dependencies, error) {
	ctx := context.Background()
	cont, redisAddr, err := prepareRedis(ctx)

	if err != nil {
		return nil, fmt.Errorf("failed to start Redis container: %w", err)
	}

	var redisOpt streamfleet.ToRedisClient
	var clientOpt streamfleet.ClientOpt
	var serverOpt streamfleet.ServerOpt
	for _, optAny := range opts {
		if rClientOpt, is := optAny.(streamfleet.RedisClientOpt); is {
			if rClientOpt.Addr == "" {
				rClientOpt.Addr = redisAddr
			}
			redisOpt = rClientOpt
		} else if failoverClientOpt, is := optAny.(streamfleet.RedisClientOpt); is {
			if failoverClientOpt.Addr == "" {
				failoverClientOpt.Addr = redisAddr
			}
			redisOpt = failoverClientOpt
		} else if clusterClientOpt, is := optAny.(streamfleet.RedisClusterClientOpt); is {
			if clusterClientOpt.Addrs == nil {
				clusterClientOpt.Addrs = []string{redisAddr}
			}
			redisOpt = clusterClientOpt
		} else if theClientOpt, is := optAny.(streamfleet.ClientOpt); is {
			clientOpt = theClientOpt
		} else if theServerOpt, is := optAny.(streamfleet.ServerOpt); is {
			serverOpt = theServerOpt
		}
	}

	if redisOpt == nil {
		redisOpt = streamfleet.RedisClientOpt{
			Addr: redisAddr,
		}
	}

	if clientOpt.RedisOpt == nil {
		clientOpt.RedisOpt = redisOpt
	}

	if serverOpt.ServerUniqueId == "" {
		serverOpt.ServerUniqueId = "test.localhost"
	}
	if serverOpt.RedisOpt == nil {
		serverOpt.RedisOpt = redisOpt
	}

	client, err := streamfleet.NewClient(ctx, clientOpt, TestQueue1)
	if err != nil {
		return nil, err
	}

	server, err := streamfleet.NewServer(serverOpt)
	if err != nil {
		return nil, err
	}

	return &dependencies{
		Container: cont,
		RedisAddr: redisAddr,
		Client:    client,
		Server:    server,
	}, nil
}

func TestSimpleEnqueueAndForget(t *testing.T) {
	d, err := mkDeps()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	err = d.Client.EnqueueAndForget(TestQueue1, "hello", streamfleet.TaskOpt{})
	if err != nil {
		t.Fatal(err)
	}

	finChan := make(chan error, 1)

	d.Server.Handle(TestQueue1, func(ctx context.Context, task *streamfleet.Task) error {
		finChan <- nil
		return nil
	})

	go func() {
		err = d.Server.Run()
		if err != nil {
			finChan <- err
		}
	}()

	err = <-finChan
	if err != nil {
		t.Fatal(err)
	}
}

func TestSimpleEnqueueAndTrack(t *testing.T) {
	d, err := mkDeps()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	handle, err := d.Client.EnqueueAndTrack(TestQueue1, "hello", streamfleet.TaskOpt{})
	if err != nil {
		t.Fatal(err)
	}

	finChan := make(chan error, 1)

	d.Server.Handle(TestQueue1, func(ctx context.Context, task *streamfleet.Task) error {
		finChan <- nil
		return nil
	})

	go func() {
		err = d.Server.Run()
		if err != nil {
			finChan <- err
		}
	}()

	// Wait for task completion.
	err = handle.Wait()
	if err != nil {
		t.Fatal(err)
	}

	err = <-finChan
	if err != nil {
		t.Fatal(err)
	}
}

func TestFailingTaskTracked(t *testing.T) {
	d, err := mkDeps()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const maxRetries = 5

	handle, err := d.Client.EnqueueAndTrack(TestQueue1, "hi", streamfleet.TaskOpt{
		MaxRetries: maxRetries,
	})
	if err != nil {
		t.Fatal(err)
	}

	finChan := make(chan error, 1)

	handlerCount := 0

	d.Server.Handle(TestQueue1, func(ctx context.Context, task *streamfleet.Task) error {
		handlerCount++

		type JsonMsg struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		var dest JsonMsg
		err := json.Unmarshal([]byte(task.Data), &dest)
		if err != nil {
			return fmt.Errorf(`json fail: %w`, err)
		}

		return nil
	})

	go func() {
		err = d.Server.Run()
		if err != nil {
			finChan <- err
		}
	}()

	// Wait for task completion.
	err = handle.Wait()
	if err == nil {
		t.Errorf("expected task to fail")
	}
	if !strings.Contains(err.Error(), "json fail") {
		t.Errorf(`expected task error message to start with "json fail", but got: %s`, err.Error())
	}

	if handlerCount != maxRetries+1 {
		t.Errorf(`max retries was set to %d, so failing handler was supposed to be called %d times, but it was called %d times`, maxRetries, maxRetries+1, handlerCount)
	}

	select {
	case err = <-finChan:
		t.Fatal(err)
	default:
	}
}

func TestFailingTaskForgotten(t *testing.T) {
	d, err := mkDeps()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const maxRetries = 5

	err = d.Client.EnqueueAndForget(TestQueue1, "hi", streamfleet.TaskOpt{
		MaxRetries: maxRetries,
	})
	if err != nil {
		t.Fatal(err)
	}

	finChan := make(chan error, 1)

	handlerCount := 0

	d.Server.Handle(TestQueue1, func(ctx context.Context, task *streamfleet.Task) error {
		handlerCount++

		type JsonMsg struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		var dest JsonMsg
		err := json.Unmarshal([]byte(task.Data), &dest)
		if err != nil {
			return fmt.Errorf(`json fail: %w`, err)
		}

		return nil
	})

	go func() {
		err = d.Server.Run()
		if err != nil {
			finChan <- err
		}
	}()

	// Wait a few seconds for task to retry.
	time.Sleep(3 * time.Second)

	if handlerCount != maxRetries+1 {
		t.Errorf(`max retries was set to %d, so failing handler was supposed to be called %d times, but it was called %d times`, maxRetries, maxRetries+1, handlerCount)
	}

	select {
	case err = <-finChan:
		t.Fatal(err)
	default:
	}
}

func TestPartiallyFailingTaskTracked(t *testing.T) {
	d, err := mkDeps()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const maxRetries = 5

	handle, err := d.Client.EnqueueAndTrack(TestQueue1, "hi", streamfleet.TaskOpt{
		MaxRetries: maxRetries,
	})
	if err != nil {
		t.Fatal(err)
	}

	finChan := make(chan error, 1)

	handlerCount := 0

	d.Server.Handle(TestQueue1, func(ctx context.Context, task *streamfleet.Task) error {
		handlerCount++

		if handlerCount == maxRetries {
			return nil
		}

		type JsonMsg struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		var dest JsonMsg
		err := json.Unmarshal([]byte(task.Data), &dest)
		if err != nil {
			return fmt.Errorf(`json fail: %w`, err)
		}

		return nil
	})

	go func() {
		err = d.Server.Run()
		if err != nil {
			finChan <- err
		}
	}()

	// Wait for task completion.
	err = handle.Wait()
	if err != nil {
		t.Errorf("task unexpectedly failed: %s", err.Error())
	}

	if handlerCount != maxRetries {
		t.Errorf(`handler was supposed to be called %d times, but it was called %d times`, maxRetries, handlerCount)
	}

	select {
	case err = <-finChan:
		t.Fatal(err)
	default:
	}
}

func TestConcurrentEnqueueAndTrack(t *testing.T) {
	const taskSleep = 2 * time.Second
	const concurrency = 4

	d, err := mkDeps(streamfleet.ServerOpt{
		HandlerConcurrency: concurrency,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	toSend := []string{"msg1", "msg2", "msg3", "msg4"}
	recvLock := sync.Mutex{}
	received := make(map[string]struct{}, len(toSend))
	handles := make([]*streamfleet.TaskHandle, len(toSend))

	for i, msg := range toSend {
		handle, err := d.Client.EnqueueAndTrack(TestQueue1, msg, streamfleet.TaskOpt{})
		if err != nil {
			t.Fatal(err)
		}
		handles[i] = handle
	}

	finChan := make(chan error, 1)

	d.Server.Handle(TestQueue1, func(ctx context.Context, task *streamfleet.Task) error {
		time.Sleep(taskSleep)

		recvLock.Lock()
		defer recvLock.Unlock()

		_, has := received[task.Data]
		if has {
			return fmt.Errorf(`expected to only receive message with data "%s" once, but it is already in the map`, task.Data)
		}

		received[task.Data] = struct{}{}

		return nil
	})

	go func() {
		err = d.Server.Run()
		if err != nil {
			finChan <- err
		}
	}()

	// Wait for task completion.
	startTime := time.Now()
	for _, handle := range handles {
		err = handle.Wait()
		if err != nil {
			t.Fatal(err)
		}
	}
	endTime := time.Now()
	if endTime.Sub(startTime) > (taskSleep + (500 * time.Millisecond)) {
		t.Errorf(`tasks took too long, it should have only taken the task sleep time, plus a padding of 500 milliseconds`)
	}

	// Check to make sure all messages were received.
	for _, msg := range toSend {
		if _, has := received[msg]; !has {
			t.Errorf(`did not receive expected message "%s"`, msg)
		}
	}

	select {
	case err = <-finChan:
		t.Fatal(err)
	default:
	}
}

func TestExpireTracked(t *testing.T) {
	d, err := mkDeps()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const taskSleep = 2 * time.Second

	// Dummy task to cause handler to wait.
	// Since by default the server has only 1 handler concurrency, this should hold up the other task.
	err = d.Client.EnqueueAndForget(TestQueue1, "wait", streamfleet.TaskOpt{})
	if err != nil {
		t.Fatal(err)
	}

	// Tasks with expiration times.
	expTime1 := time.Now().Add(taskSleep)
	handle1, err := d.Client.EnqueueAndTrack(TestQueue1, "can_expire", streamfleet.TaskOpt{
		ExpiresTs: &expTime1,
	})
	if err != nil {
		t.Fatal(err)
	}
	expTime2 := time.Now().Add(taskSleep + 1*time.Second)
	handle2, err := d.Client.EnqueueAndTrack(TestQueue1, "can_expire", streamfleet.TaskOpt{
		ExpiresTs: &expTime2,
	})
	if err != nil {
		t.Fatal(err)
	}

	finChan := make(chan error, 1)

	d.Server.Handle(TestQueue1, func(ctx context.Context, task *streamfleet.Task) error {
		if task.Data == "wait" {
			time.Sleep(taskSleep)
		}

		return nil
	})

	go func() {
		err = d.Server.Run()
		if err != nil {
			finChan <- err
		}
	}()

	// Wait for task completions.
	err = handle1.Wait()
	if err == nil {
		t.Errorf(`expected first task to fail`)
	}
	if !errors.Is(err, streamfleet.ErrTaskExpired) {
		t.Errorf(`expected first task to fail because of expiration, instead got error: %s`, err.Error())
	}
	err = handle2.Wait()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err = <-finChan:
		t.Fatal(err)
	default:
	}
}

func TestDelayedServerStart(t *testing.T) {
	d, err := mkDeps()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const delay = 2 * time.Second

	handle, err := d.Client.EnqueueAndTrack(TestQueue1, "hi", streamfleet.TaskOpt{})
	if err != nil {
		t.Fatal(err)
	}

	finChan := make(chan error, 1)

	d.Server.Handle(TestQueue1, func(ctx context.Context, task *streamfleet.Task) error {
		return nil
	})

	go func() {
		time.Sleep(delay)
		err = d.Server.Run()
		if err != nil {
			finChan <- err
		}
	}()

	// Wait for task completion.
	startTime := time.Now()
	err = handle.Wait()
	endTime := time.Now()
	if err != nil {
		t.Fatal(err)
	}

	elapsed := endTime.Sub(startTime)
	if elapsed > delay+(500*time.Millisecond) || elapsed < delay {
		t.Errorf(`unexpected delay between enqueuing task and its completion, elapsed time was %s`, elapsed.String())
	}

	select {
	case err = <-finChan:
		t.Fatal(err)
	default:
	}
}

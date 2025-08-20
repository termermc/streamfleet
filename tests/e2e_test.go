package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/termermc/streamfleet"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"math/rand/v2"
	"strconv"
	"strings"
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

func mkDeps() (*dependencies, error) {
	ctx := context.Background()
	cont, redisAddr, err := prepareRedis(ctx)

	if err != nil {
		return nil, fmt.Errorf("failed to start Redis container: %w", err)
	}

	redisOpt := streamfleet.RedisClientOpt{
		Addr: redisAddr,
	}

	client, err := streamfleet.NewClient(ctx, streamfleet.ClientOpt{
		RedisOpt: redisOpt,
	}, TestQueue1)
	if err != nil {
		return nil, err
	}

	server, err := streamfleet.NewServer(streamfleet.ServerOpt{
		ServerUniqueId: "test.localhost",
		RedisOpt:       redisOpt,
	})
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

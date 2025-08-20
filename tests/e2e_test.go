package tests

import (
	"context"
	"fmt"
	"github.com/termermc/streamfleet"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"math/rand/v2"
	"strconv"
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

func TestServer(t *testing.T) {
	ctx := context.Background()
	cont, redisAddr, err := prepareRedis(ctx)
	defer func() {
		// Give time for server and client to close.
		time.Sleep(1 * time.Second)
		_ = cont.Stop(context.Background(), nil)
	}()

	if err != nil {
		t.Fatal(fmt.Errorf("failed to start Redis container: %w", err))
	}

	redisOpt := streamfleet.RedisClientOpt{
		Addr: redisAddr,
	}

	client, err := streamfleet.NewClient(ctx, streamfleet.ClientOpt{
		RedisOpt: redisOpt,
	}, TestQueue1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
	}()

	err = client.EnqueueAndForget(TestQueue1, "hello", streamfleet.TaskOpt{})
	if err != nil {
		t.Fatal(err)
	}

	server, err := streamfleet.NewServer(streamfleet.ServerOpt{
		ServerUniqueId: "test.localhost",
		RedisOpt:       redisOpt,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = server.Close()
	}()

	finChan := make(chan error, 1)

	server.Handle(TestQueue1, func(ctx context.Context, task *streamfleet.Task) error {
		finChan <- nil
		return nil
	})

	go func() {
		err = server.Run()
		if err != nil {
			finChan <- err
		}
	}()

	err = <-finChan
	if err != nil {
		t.Fatal(err)
	}
}

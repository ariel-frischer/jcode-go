package jcode

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
	"github.com/ariel-frischer/jcode-go/transport"
)

type trackingInstance struct {
	shutdowns atomic.Int32
}

func (i *trackingInstance) SocketPath() string { return "/tmp/safe-run-test.sock" }
func (i *trackingInstance) JcodeHome() string  { return "/tmp/safe-run-test-home" }
func (i *trackingInstance) Shutdown() error {
	i.shutdowns.Add(1)
	return nil
}
func (i *trackingInstance) Close() error { return i.Shutdown() }

type blockingTrackingInstance struct {
	shutdowns atomic.Int32
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (i *blockingTrackingInstance) SocketPath() string { return "/tmp/safe-run-blocking.sock" }
func (i *blockingTrackingInstance) JcodeHome() string  { return "/tmp/safe-run-blocking-home" }
func (i *blockingTrackingInstance) Shutdown() error {
	i.shutdowns.Add(1)
	i.once.Do(func() { close(i.started) })
	<-i.release
	return nil
}
func (i *blockingTrackingInstance) Close() error { return i.Shutdown() }

func TestDetachInstanceTransfersPrivateRuntimeOwnership(t *testing.T) {
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go serveHello(server)

	client, err := NewClient(ctx, clientSide, Options{})
	if err != nil {
		t.Fatal(err)
	}
	instance := &trackingInstance{}
	client.setInstance(instance)

	detached, ok := client.DetachInstance()
	if !ok || detached != instance {
		t.Fatalf("DetachInstance() = (%v, %v), want (%v, true)", detached, ok, instance)
	}
	if _, ok := client.DetachInstance(); ok {
		t.Fatal("second DetachInstance unexpectedly transferred ownership")
	}
	if err := client.Close(); err != ErrClosed {
		t.Fatal(err)
	}
	if instance.shutdowns.Load() != 0 {
		t.Fatalf("client close shut down detached instance %d times", instance.shutdowns.Load())
	}
	if err := detached.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if instance.shutdowns.Load() != 1 {
		t.Fatalf("owner shutdown count=%d, want 1", instance.shutdowns.Load())
	}
}

func TestClientCloseAndDetachSerializeInstanceOwnership(t *testing.T) {
	for iteration := 0; iteration < 200; iteration++ {
		instance := &trackingInstance{}
		client := &Client{
			state: StateConnected, closed: make(chan struct{}),
			pending: make(map[uint64]pendingRequest),
			subs:    make(map[uint64]*subscriber), instance: instance,
		}
		start := make(chan struct{})
		var detached Instance
		var detachedOK bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = client.Close()
		}()
		go func() {
			defer wg.Done()
			<-start
			detached, detachedOK = client.DetachInstance()
		}()
		close(start)
		wg.Wait()

		shutdowns := instance.shutdowns.Load()
		switch {
		case detachedOK:
			if detached != instance {
				t.Fatalf("iteration %d detached %v, want instance", iteration, detached)
			}
			if shutdowns != 0 {
				t.Fatalf("iteration %d returned and shut down instance %d times", iteration, shutdowns)
			}
		case shutdowns != 1:
			t.Fatalf("iteration %d close-owned shutdowns = %d, want 1", iteration, shutdowns)
		}
	}
}

func TestClientCloseClaimsInstanceBeforeRunningShutdown(t *testing.T) {
	instance := &blockingTrackingInstance{started: make(chan struct{}), release: make(chan struct{})}
	client := &Client{
		state: StateConnected, closed: make(chan struct{}),
		pending: make(map[uint64]pendingRequest),
		subs:    make(map[uint64]*subscriber), instance: instance,
	}
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case <-instance.started:
	case <-time.After(time.Second):
		t.Fatal("client close did not begin instance shutdown")
	}
	if detached, ok := client.DetachInstance(); ok || detached != nil {
		t.Fatalf("DetachInstance() during close = (%v, %v), want (nil, false)", detached, ok)
	}
	close(instance.release)
	select {
	case err := <-closed:
		if err != ErrClosed {
			t.Fatalf("Client.Close() error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client close did not finish after instance shutdown release")
	}
	if instance.shutdowns.Load() != 1 {
		t.Fatalf("instance shutdowns = %d, want 1", instance.shutdowns.Load())
	}
}

func TestConnectClientHasNoInstanceOwnership(t *testing.T) {
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go serveHello(server)

	client, err := NewClient(ctx, clientSide, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if instance, ok := client.DetachInstance(); ok || instance != nil {
		t.Fatalf("shared client detached instance (%v, %v)", instance, ok)
	}
	if err := client.Close(); err != ErrClosed {
		t.Fatal(err)
	}
}

func serveHello(server *transport.FakeServer) {
	frame, err := server.Receive()
	if err != nil {
		return
	}
	var request protocol.ClientFrame
	if json.Unmarshal(frame, &request) != nil {
		return
	}
	response, err := protocol.EncodeServerFrame(1, &request.ID, "hello_ok", map[string]any{"version": 1, "server": "test"})
	if err == nil {
		_ = server.Send(response)
	}
}

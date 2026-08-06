package jcode

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
	"github.com/ariel-frischer/jcode-go/transport"
)

type trackingInstance struct {
	shutdowns int
}

func (i *trackingInstance) SocketPath() string { return "/tmp/safe-run-test.sock" }
func (i *trackingInstance) JcodeHome() string  { return "/tmp/safe-run-test-home" }
func (i *trackingInstance) Shutdown() error {
	i.shutdowns++
	return nil
}
func (i *trackingInstance) Close() error { return i.Shutdown() }

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
	if instance.shutdowns != 0 {
		t.Fatalf("client close shut down detached instance %d times", instance.shutdowns)
	}
	if err := detached.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if instance.shutdowns != 1 {
		t.Fatalf("owner shutdown count=%d, want 1", instance.shutdowns)
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

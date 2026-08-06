//go:build !windows

package jcode

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
)

func TestConnectUsesExplicitSocketAndExposesCapabilities(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "api.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		requestBytes, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			serverDone <- err
			return
		}
		var request protocol.ClientFrame
		if err := json.Unmarshal(requestBytes, &request); err != nil {
			serverDone <- err
			return
		}
		reply, err := protocol.EncodeServerFrame(protocol.APIVersionMajor, &request.ID, "hello_ok", map[string]any{
			"version":      protocol.APIVersionMajor,
			"server":       "test",
			"capabilities": []string{"resume", "attach_session"},
		})
		if err != nil {
			serverDone <- err
			return
		}
		_, err = conn.Write(append(reply, '\n'))
		serverDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := Connect(ctx, ConnectOptions{SocketPath: socketPath, ClientOptions: Options{ClientName: "connect-test"}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if client.State() != StateConnected {
		t.Fatalf("state=%q", client.State())
	}
	if got, want := client.Capabilities(), []string{"attach_session", "resume"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("capabilities=%v, want %v", got, want)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestConnectUsesEnvironmentSocketWhenExplicitPathIsAbsent(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "api.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("JCODE_API_SOCKET", socketPath)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		requestBytes, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			return
		}
		var request protocol.ClientFrame
		if json.Unmarshal(requestBytes, &request) != nil {
			return
		}
		reply, _ := protocol.EncodeServerFrame(protocol.APIVersionMajor, &request.ID, "hello_ok", map[string]any{"version": 1})
		_, _ = conn.Write(append(reply, '\n'))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := Connect(ctx, ConnectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.State() != StateConnected {
		t.Fatalf("state=%q", client.State())
	}
	_ = os.Getenv("JCODE_API_SOCKET")
}

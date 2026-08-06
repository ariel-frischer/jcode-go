package transport

import (
	"bytes"
	"testing"
)

func TestPipePairAndFakeServerPreserveFrames(t *testing.T) {
	client, server := NewPipePair()
	fixture := NewFakeServer(server)
	defer client.Close()
	defer fixture.Close()

	request := []byte(`{"v":1,"id":7,"req":"ping"}` + "\n")
	go func() { _, _ = client.Write(request) }()
	got, err := fixture.Receive()
	if err != nil || !bytes.Equal(got, request) {
		t.Fatalf("receive=%q err=%v", got, err)
	}

	reply := []byte(`{"v":1,"reply_to":7,"ev":"pong"}`)
	go func() { _ = fixture.Send(reply) }()
	got, err = clientReadLine(client)
	if err != nil || !bytes.Equal(got, append(reply, '\n')) {
		t.Fatalf("reply=%q err=%v", got, err)
	}
}

func clientReadLine(client *Pipe) ([]byte, error) {
	var one [1]byte
	out := make([]byte, 0, 64)
	for {
		n, err := client.Read(one[:])
		if n > 0 {
			out = append(out, one[0])
			if one[0] == '\n' {
				return out, nil
			}
		}
		if err != nil {
			return out, err
		}
	}
}

func TestFakeServerCloseUnblocksStream(t *testing.T) {
	client, server := NewPipePair()
	fixture := NewFakeServer(server)
	closed := make(chan error, 1)
	go func() { _, err := fixture.Receive(); closed <- err }()
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err == nil {
		t.Fatal("expected receive error after close")
	}
	_ = client.Close()
}

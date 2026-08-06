package transport

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
)

var (
	ErrClosed      = errors.New("transport closed")
	ErrUnsupported = errors.New("transport unsupported on this operating system")
)

type Transport interface{ io.ReadWriteCloser }

// Factory creates a fresh transport. It is used by the SDK's explicit
// reconnect operation. Factories must not reuse a transport after Close.
type Factory func(context.Context) (Transport, error)

// Safe serializes writes and makes Close idempotent for transports whose
// implementations do not provide those guarantees themselves.
type Safe struct {
	Transport
	writeMu sync.Mutex
	once    sync.Once
	err     error
}

func NewSafe(t Transport) Transport {
	if t == nil {
		return nil
	}
	return &Safe{Transport: t}
}

func (s *Safe) Write(p []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.Transport.Write(p)
}

func (s *Safe) Close() error {
	s.once.Do(func() { s.err = s.Transport.Close() })
	return s.err
}

type Pipe struct {
	reader *io.PipeReader
	writer *io.PipeWriter
	once   sync.Once
}

func NewPipePair() (*Pipe, *Pipe) {
	leftRead, rightWrite := io.Pipe()
	rightRead, leftWrite := io.Pipe()
	return &Pipe{reader: leftRead, writer: leftWrite}, &Pipe{reader: rightRead, writer: rightWrite}
}
func (p *Pipe) Read(b []byte) (int, error)  { return p.reader.Read(b) }
func (p *Pipe) Write(b []byte) (int, error) { return p.writer.Write(b) }
func (p *Pipe) Close() error {
	var err error
	p.once.Do(func() { _ = p.reader.Close(); err = p.writer.Close() })
	return err
}

type FakeServer struct {
	side   *Pipe
	reader *bufio.Reader
	done   chan struct{}
}

func NewFakeServer(side *Pipe) *FakeServer {
	return &FakeServer{side: side, reader: bufio.NewReader(side), done: make(chan struct{})}
}
func (s *FakeServer) Send(raw []byte) error {
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		raw = append(bytes.Clone(raw), '\n')
	}
	_, err := s.side.Write(raw)
	return err
}
func (s *FakeServer) Receive() ([]byte, error) { return s.reader.ReadBytes('\n') }
func (s *FakeServer) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return s.side.Close()
}

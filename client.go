package jcode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
	"github.com/ariel-frischer/jcode-go/transport"
)

var (
	ErrClosed             = errors.New("jcode client closed")
	ErrDisconnected       = errors.New("jcode client disconnected")
	ErrSubscriberOverflow = errors.New("jcode event subscriber fell behind")
	ErrCapability         = errors.New("jcode capability is not supported")
	ErrResume             = errors.New("jcode session resume failed")
)

type State string

const (
	StateConnecting   State = "connecting"
	StateConnected    State = "connected"
	StateDisconnected State = "disconnected"
	StateReconnecting State = "reconnecting"
	StateClosing      State = "closing"
	StateClosed       State = "closed"
)

// Observation contains only metadata. It intentionally excludes request fields,
// prompts, credentials, server messages, and session identifiers.
type Observation struct {
	Kind     string
	State    State
	Request  string
	Error    string
	Attempts int
}

// Observer receives redacted lifecycle metadata. Implementations must be safe
// for concurrent calls and should return quickly. Request lifecycle events are
// emitted as request_write_start, request_write_complete, request_reply, or
// request_timeout, so a missing phase identifies the stalled boundary without
// exposing payloads, credentials, or identifiers.
type Observer interface{ Observe(Observation) }

type ReconnectPolicy struct {
	Factory     transport.Factory
	MaxAttempts int
	Backoff     time.Duration
	MaxBackoff  time.Duration
	Resume      bool
}

// Options controls client construction and event buffering.
type Options struct {
	ClientName    string
	MaxFrameSize  int
	EventBuffer   int
	RequestBuffer int
	// RequestTimeout bounds each request when the caller's context has no
	// earlier deadline. Non-positive values use the 30-second default.
	RequestTimeout time.Duration
	SessionID      string
	Reconnect      ReconnectPolicy
	Observer       Observer
}

func (o Options) withDefaults() Options {
	if o.ClientName == "" {
		o.ClientName = protocol.DefaultClient
	}
	if o.EventBuffer <= 0 {
		o.EventBuffer = 128
	}
	if o.RequestBuffer <= 0 {
		o.RequestBuffer = 1
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 30 * time.Second
	}
	if o.Reconnect.MaxAttempts <= 0 {
		o.Reconnect.MaxAttempts = 1
	}
	if o.Reconnect.MaxBackoff <= 0 {
		o.Reconnect.MaxBackoff = 30 * time.Second
	}
	return o
}

// Event is a server event with its stable kind and forward-compatible fields.
type Event struct {
	Frame  protocol.ServerFrame
	Kind   string
	Fields json.RawMessage
}

func (e Event) Decode(value any) error {
	if len(e.Fields) == 0 {
		return nil
	}
	return json.Unmarshal(e.Fields, value)
}

type Subscription struct {
	client *Client
	id     uint64
	sub    *subscriber
	events <-chan Event
	errors <-chan error
	once   sync.Once
}

func (s *Subscription) Next(ctx context.Context) (Event, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case event, ok := <-s.events:
		if !ok {
			return Event{}, s.client.subscriptionError(s.id, s.sub)
		}
		return event, nil
	case err, ok := <-s.errors:
		if !ok {
			return Event{}, s.client.subscriptionError(s.id, s.sub)
		}
		return Event{}, err
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}

func (s *Subscription) Close() { s.once.Do(func() { s.client.unsubscribe(s.id) }) }

type Client struct {
	transportMu  sync.RWMutex
	transport    transport.Transport
	encoder      *protocol.Encoder
	decoder      *protocol.Decoder
	options      Options
	reconnectMu  sync.Mutex
	stateMu      sync.RWMutex
	state        State
	capMu        sync.RWMutex
	capabilities map[string]struct{}
	sessionID    string
	writeMu      sync.Mutex
	pendingMu    sync.Mutex
	pending      map[uint64]chan protocol.ServerFrame
	subsMu       sync.Mutex
	subs         map[uint64]*subscriber
	nextID       atomic.Uint64
	nextSub      atomic.Uint64
	closed       chan struct{}
	closeOnce    sync.Once
	closeErr     error
	instance     Instance
}

func (c *Client) setInstance(instance Instance) {
	c.stateMu.Lock()
	c.instance = instance
	c.stateMu.Unlock()
}

// DetachInstance transfers ownership of a private runtime from the client to
// the caller. After a successful detach, closing the client only closes its
// protocol transport. The returned Instance remains responsible for the
// private process, daemon, and SDK-owned state until Shutdown is called.
//
// This is the safe-run boundary for applications that let a worker client
// come and go while a session owner supervises the runtime independently.
func (c *Client) DetachInstance() (Instance, bool) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.instance == nil || c.state == StateClosing || c.state == StateClosed {
		return nil, false
	}
	instance := c.instance
	c.instance = nil
	return instance, true
}

type subscriber struct {
	events chan Event
	errors chan error
	done   chan struct{}
	once   sync.Once
	err    error
}

// NewClient starts reading from t and completes the protocol hello handshake.
// The transport is closed if the handshake fails. Reconnect is always explicit.
func NewClient(ctx context.Context, t transport.Transport, options Options) (*Client, error) {
	if t == nil {
		return nil, errors.New("nil transport")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	o := options.withDefaults()
	c := &Client{
		transport: transport.NewSafe(t), options: o, state: StateConnecting,
		capabilities: make(map[string]struct{}), sessionID: o.SessionID,
		pending: make(map[uint64]chan protocol.ServerFrame), subs: make(map[uint64]*subscriber),
		closed: make(chan struct{}),
	}
	c.installDecoder(c.transport)
	c.emit(Observation{Kind: "state", State: StateConnecting})
	go c.readLoop(c.decoder)
	if err := c.handshake(ctx); err != nil {
		c.Close()
		return nil, err
	}
	c.setState(StateConnected)
	return c, nil
}

func (c *Client) installDecoder(t transport.Transport) {
	c.transportMu.Lock()
	c.transport = t
	c.encoder = protocol.NewEncoder(t)
	c.decoder = protocol.NewDecoder(t)
	if c.options.MaxFrameSize > 0 {
		c.encoder.MaxSize = c.options.MaxFrameSize
		c.decoder.MaxSize = c.options.MaxFrameSize
	}
	decoder := c.decoder
	c.transportMu.Unlock()
	_ = decoder
}

func (c *Client) handshake(ctx context.Context) error {
	fields := struct {
		MinVersion int    `json:"min_version"`
		MaxVersion int    `json:"max_version"`
		Client     string `json:"client"`
	}{1, 1, c.options.ClientName}
	req, err := protocol.NewRawRequest("hello", fields)
	if err != nil {
		return err
	}
	frame, err := c.request(ctx, req, true)
	if err != nil {
		return err
	}
	if value, ok := frame.Event.(protocol.Error); ok {
		return fmt.Errorf("hello failed: %s: %s", value.Code, value.Message)
	}
	hello, ok := frame.Event.(protocol.HelloOK)
	if !ok {
		return fmt.Errorf("unexpected hello reply: %T", frame.Event)
	}
	if !protocol.IsCompatibleVersion(hello.Version) {
		return fmt.Errorf("unsupported server API version %d", hello.Version)
	}
	c.capMu.Lock()
	c.capabilities = make(map[string]struct{}, len(hello.Capabilities))
	for _, capability := range hello.Capabilities {
		c.capabilities[capability] = struct{}{}
	}
	c.capMu.Unlock()
	return nil
}

func (c *Client) request(ctx context.Context, req protocol.RawRequest, internal ...bool) (protocol.ServerFrame, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	requestCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
	defer cancel()
	allowConnecting := len(internal) > 0 && internal[0]
	if !allowConnecting && c.State() != StateConnected {
		return protocol.ServerFrame{}, c.requestStateError()
	}
	select {
	case <-c.closed:
		return protocol.ServerFrame{}, ErrClosed
	default:
	}
	id := c.nextID.Add(1)
	reply := make(chan protocol.ServerFrame, 1)
	c.pendingMu.Lock()
	c.pending[id] = reply
	c.pendingMu.Unlock()
	frame := protocol.ClientFrame{V: protocol.APIVersionMajor, ID: id, Request: req}
	c.writeMu.Lock()
	c.transportMu.RLock()
	encoder := c.encoder
	c.transportMu.RUnlock()
	c.emit(Observation{Kind: "request_write_start", Request: req.Req})
	err := encoder.Write(frame)
	c.writeMu.Unlock()
	// A missing write_complete points at a blocked transport write. A later
	// request_timeout proves the frame was sent but never answered.
	if err == nil {
		c.emit(Observation{Kind: "request_write_complete", Request: req.Req})
	} else {
		c.emit(Observation{Kind: "request_write_error", Request: req.Req, Error: errorKind(err)})
	}
	c.emit(Observation{Kind: "request", Request: req.Req})
	if err != nil {
		c.removePending(id)
		c.disconnect(err, false)
		return protocol.ServerFrame{}, err
	}
	select {
	case result, ok := <-reply:
		if !ok {
			c.emit(Observation{Kind: "request_error", Request: req.Req, Error: "disconnected"})
			return protocol.ServerFrame{}, ErrDisconnected
		}
		c.emit(Observation{Kind: "request_reply", Request: req.Req})
		return result, nil
	case <-requestCtx.Done():
		c.removePending(id)
		c.emit(Observation{Kind: "request_timeout", Request: req.Req, Error: requestContextError(requestCtx.Err())})
		return protocol.ServerFrame{}, requestCtx.Err()
	case <-c.closed:
		c.removePending(id)
		c.emit(Observation{Kind: "request_error", Request: req.Req, Error: "closed"})
		return protocol.ServerFrame{}, ErrClosed
	}
}

func requestContextError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "context_done"
	}
}

func (c *Client) requestStateError() error {
	if c.State() == StateClosed {
		return ErrClosed
	}
	return ErrDisconnected
}

// Request sends a raw request. It never retries, preserving caller control for
// non-idempotent operations such as send_message.
func (c *Client) Request(ctx context.Context, req protocol.RawRequest) (protocol.ServerFrame, error) {
	return c.request(ctx, req)
}

// Notify writes a request without waiting for a correlated request-level reply.
// Use this for protocol operations whose result is delivered as an event.
func (c *Client) Notify(req protocol.RawRequest) error {
	if c.State() != StateConnected {
		return c.requestStateError()
	}
	select {
	case <-c.closed:
		return ErrClosed
	default:
	}
	id := c.nextID.Add(1)
	frame := protocol.ClientFrame{V: protocol.APIVersionMajor, ID: id, Request: req}
	c.writeMu.Lock()
	c.transportMu.RLock()
	encoder := c.encoder
	c.transportMu.RUnlock()
	err := encoder.Write(frame)
	c.writeMu.Unlock()
	c.emit(Observation{Kind: "request", Request: req.Req})
	if err != nil {
		c.disconnect(err, false)
	}
	return err
}

// Supports reports whether the server advertised capability.
func (c *Client) Supports(capability string) bool {
	c.capMu.RLock()
	_, ok := c.capabilities[capability]
	c.capMu.RUnlock()
	return ok
}

// Capabilities returns the server-advertised capabilities in stable order.
// The returned slice is a copy and may be modified by the caller.
func (c *Client) Capabilities() []string {
	c.capMu.RLock()
	capabilities := make([]string, 0, len(c.capabilities))
	for capability := range c.capabilities {
		capabilities = append(capabilities, capability)
	}
	c.capMu.RUnlock()
	sort.Strings(capabilities)
	return capabilities
}

// RequireCapability returns a stable error before a capability-gated request.
func (c *Client) RequireCapability(capability string) error {
	if c.Supports(capability) {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrCapability, capability)
}

// RequestCapability sends req only when the server advertised capability.
// Unlike Request, this performs a local check and never writes an unsupported
// request to the wire.
func (c *Client) RequestCapability(ctx context.Context, capability string, req protocol.RawRequest) (protocol.ServerFrame, error) {
	if err := c.RequireCapability(capability); err != nil {
		return protocol.ServerFrame{}, err
	}
	return c.Request(ctx, req)
}

// SessionID returns the caller-selected identity retained across reconnects.
func (c *Client) SessionID() string { c.stateMu.RLock(); defer c.stateMu.RUnlock(); return c.sessionID }
func (c *Client) SetSessionID(sessionID string) {
	c.stateMu.Lock()
	c.sessionID = sessionID
	c.stateMu.Unlock()
}
func (c *Client) State() State { c.stateMu.RLock(); defer c.stateMu.RUnlock(); return c.state }

// Subscribe receives asynchronous events. A full buffer terminates only that
// subscription, keeping the reader bounded and other callers live.
func (c *Client) Subscribe(sessionID string) *Subscription {
	buffer := c.options.EventBuffer
	s := &subscriber{events: make(chan Event, buffer), errors: make(chan error, 1), done: make(chan struct{})}
	id := c.nextSub.Add(1)
	c.subsMu.Lock()
	select {
	case <-c.closed:
		s.err = ErrClosed
		close(s.events)
		close(s.errors)
	default:
		c.subs[id] = s
	}
	c.subsMu.Unlock()
	return &Subscription{client: c, id: id, sub: s, events: s.events, errors: s.errors}
}

func (c *Client) readLoop(decoder *protocol.Decoder) {
	for {
		data, err := decoder.ReadFrame()
		if err != nil {
			if c.State() != StateClosed && c.State() != StateClosing {
				terminal := errors.Is(err, protocol.ErrMalformedFrame) || errors.Is(err, protocol.ErrFrameTooLarge)
				c.disconnect(err, terminal)
			}
			return
		}
		frame, err := protocol.DecodeServerFrame(data)
		if err != nil {
			c.disconnect(err, true)
			return
		}
		if frame.ReplyTo != nil {
			c.pendingMu.Lock()
			reply := c.pending[*frame.ReplyTo]
			if reply != nil {
				delete(c.pending, *frame.ReplyTo)
			}
			c.pendingMu.Unlock()
			if reply != nil {
				reply <- frame
			}
			continue
		}
		fields, _ := protocol.FieldsJSON(frame.Event)
		event := Event{Frame: frame, Kind: eventKind(frame.Event), Fields: fields}
		c.subsMu.Lock()
		for id, sub := range c.subs {
			select {
			case <-sub.done:
				delete(c.subs, id)
			case sub.events <- event:
			default:
				c.failSubscriberLocked(id, sub, ErrSubscriberOverflow)
			}
		}
		c.subsMu.Unlock()
	}
}

func eventKind(event protocol.Event) string {
	switch value := event.(type) {
	case protocol.HelloOK:
		return "hello_ok"
	case protocol.OK:
		return "ok"
	case protocol.Error:
		return "error"
	case protocol.RawEvent:
		return value.Kind
	case protocol.UnknownEvent:
		return value.Kind
	default:
		return ""
	}
}

func (c *Client) setState(state State) {
	c.stateMu.Lock()
	c.state = state
	c.stateMu.Unlock()
	c.emit(Observation{Kind: "state", State: state})
}
func (c *Client) emit(observation Observation) {
	if c.options.Observer != nil {
		c.options.Observer.Observe(observation)
	}
}

func (c *Client) removePending(id uint64) {
	c.pendingMu.Lock()
	delete(c.pending, id)
	c.pendingMu.Unlock()
}
func (c *Client) unsubscribe(id uint64) {
	c.subsMu.Lock()
	if sub := c.subs[id]; sub != nil {
		c.closeSubscriberLocked(id, sub, nil)
	}
	c.subsMu.Unlock()
}
func (c *Client) subscriptionError(id uint64, fallback *subscriber) error {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	if sub := c.subs[id]; sub != nil && sub.err != nil {
		return sub.err
	}
	if fallback != nil && fallback.err != nil {
		return fallback.err
	}
	if c.State() == StateClosed {
		return ErrClosed
	}
	return ErrDisconnected
}
func (c *Client) failSubscriberLocked(id uint64, sub *subscriber, err error) {
	c.closeSubscriberLocked(id, sub, err)
}
func (c *Client) closeSubscriberLocked(id uint64, sub *subscriber, err error) {
	delete(c.subs, id)
	sub.once.Do(func() {
		sub.err = err
		if err != nil {
			sub.errors <- err
		}
		close(sub.done)
		close(sub.events)
		close(sub.errors)
	})
}

func (c *Client) disconnect(err error, terminal bool) {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	if c.State() == StateClosed || c.State() == StateClosing || (c.State() == StateDisconnected && !terminal) {
		return
	}
	if c.State() != StateDisconnected {
		c.setState(StateDisconnected)
	}
	c.emit(Observation{Kind: "disconnect", Error: errorKind(err)})
	c.transportMu.RLock()
	t := c.transport
	c.transportMu.RUnlock()
	if t != nil {
		_ = t.Close()
	}
	c.pendingMu.Lock()
	pending := c.pending
	c.pending = make(map[uint64]chan protocol.ServerFrame)
	c.pendingMu.Unlock()
	for _, ch := range pending {
		close(ch)
	}
	if terminal {
		c.subsMu.Lock()
		for id, sub := range c.subs {
			c.closeSubscriberLocked(id, sub, err)
		}
		c.subsMu.Unlock()
	}
}

func errorKind(err error) string {
	switch {
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, protocol.ErrFrameTooLarge):
		return "frame_too_large"
	case errors.Is(err, transport.ErrClosed):
		return "transport_closed"
	default:
		return "transport_error"
	}
}

// Reconnect explicitly reconnects and, when configured, safely reattaches the
// remembered session. In-flight requests are never retried.
func (c *Client) Reconnect(ctx context.Context) error {
	policy := c.options.Reconnect
	if policy.Factory == nil {
		return errors.New("reconnect factory is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	if c.State() == StateClosed {
		return ErrClosed
	}
	c.setState(StateReconnecting)
	var last error
	backoff := policy.Backoff
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if attempt > 1 && backoff > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				c.setState(StateDisconnected)
				return ctx.Err()
			}
			if backoff < policy.MaxBackoff {
				backoff *= 2
				if backoff > policy.MaxBackoff {
					backoff = policy.MaxBackoff
				}
			}
		}
		t, err := policy.Factory(ctx)
		if err != nil {
			last = err
			c.emit(Observation{Kind: "reconnect_failed", Error: errorKind(err), Attempts: attempt})
			continue
		}
		t = transport.NewSafe(t)
		c.installDecoder(t)
		c.setState(StateConnecting)
		go c.readLoop(c.decoder)
		if err = c.handshake(ctx); err != nil {
			last = err
			_ = t.Close()
			c.setState(StateDisconnected)
			c.emit(Observation{Kind: "reconnect_failed", Error: errorKind(err), Attempts: attempt})
			continue
		}
		if policy.Resume && c.SessionID() != "" {
			if err = c.resume(ctx); err != nil {
				last = err
				_ = t.Close()
				c.setState(StateDisconnected)
				c.emit(Observation{Kind: "resume_failed", Error: errorKind(err), Attempts: attempt})
				continue
			}
		}
		c.setState(StateConnected)
		c.emit(Observation{Kind: "reconnected", Attempts: attempt})
		return nil
	}
	if last == nil {
		last = ErrDisconnected
	}
	return last
}

func (c *Client) resume(ctx context.Context) error {
	if err := c.RequireCapability("attach_session"); err != nil {
		return fmt.Errorf("%w: %v", ErrResume, err)
	}
	fields, err := protocol.NewRawRequest("attach_session", struct {
		SessionID string `json:"session_id"`
	}{c.SessionID()})
	if err != nil {
		return err
	}
	frame, err := c.request(ctx, fields, true)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrResume, err)
	}
	if _, ok := frame.Event.(protocol.Error); ok {
		return fmt.Errorf("%w: server rejected session", ErrResume)
	}
	return nil
}

func (c *Client) closeWith(err error) {
	c.closeOnce.Do(func() {
		c.stateMu.Lock()
		c.state = StateClosing
		instance := c.instance
		c.instance = nil
		c.stateMu.Unlock()
		c.emit(Observation{Kind: "state", State: StateClosing})
		c.closeErr = err
		close(c.closed)
		c.transportMu.RLock()
		t := c.transport
		c.transportMu.RUnlock()
		if t != nil {
			_ = t.Close()
		}
		c.pendingMu.Lock()
		pending := c.pending
		c.pending = make(map[uint64]chan protocol.ServerFrame)
		c.pendingMu.Unlock()
		for _, ch := range pending {
			close(ch)
		}
		c.subsMu.Lock()
		for id, sub := range c.subs {
			c.closeSubscriberLocked(id, sub, err)
		}
		c.subsMu.Unlock()
		c.setState(StateClosed)
		if instance != nil {
			_ = instance.Shutdown()
		}
	})
}

// Close is idempotent and wakes all pending requests and subscriptions.
func (c *Client) Close() error { c.closeWith(ErrClosed); return c.closeErr }

// RedactedID can be used by applications to correlate a session without
// placing the raw identifier in logs.
func RedactedID(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:12]
}

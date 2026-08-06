//go:build !windows

package transport

import (
	"context"
	"net"
)

// UnixSocket returns a factory for a local Unix-domain socket. Each call
// creates a new connection, which makes it safe to use with Client.Reconnect.
func UnixSocket(path string) Factory {
	return func(ctx context.Context) (Transport, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "unix", path)
		if err != nil {
			return nil, err
		}
		return NewSafe(conn), nil
	}
}

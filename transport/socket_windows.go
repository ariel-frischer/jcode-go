//go:build windows

package transport

import (
	"context"
)

// UnixSocket is intentionally unavailable on Windows. Use a caller-provided
// named-pipe or TCP transport until a supported named-pipe adapter is added.
func UnixSocket(path string) Factory {
	return func(context.Context) (Transport, error) {
		return nil, ErrUnsupported
	}
}

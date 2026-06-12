package client

import (
	"context"
	"net"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/go-spdk-helper/pkg/types"
)

type Client struct {
	conn net.Conn

	jsonCli *jsonrpc.Client
}

func NewClient(ctx context.Context) (*Client, error) {
	return NewClientWithDefaultTimeout(ctx, 0)
}

// NewClientWithDefaultTimeout is NewClient with a custom default timeout
// applied to every standard (non-long-timeout) RPC issued through the
// returned client. A non-positive defaultTimeout keeps the package default
// (jsonrpc.DefaultShortTimeout). Useful for time-boxed callers such as
// PreStop hooks that must not block for the full 60s default against a
// wedged-but-accepting SPDK target.
func NewClientWithDefaultTimeout(ctx context.Context, defaultTimeout time.Duration) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, types.DefaultJSONServerNetwork, types.DefaultUnixDomainSocketPath)
	if err != nil {
		return nil, errors.Wrap(err, "error opening socket for spdk client")
	}

	return &Client{
		conn:    conn,
		jsonCli: jsonrpc.NewClientWithTimeout(ctx, conn, defaultTimeout),
	}, nil
}

// NewClientWithConn builds a Client over an already-established connection
// instead of dialing the default SPDK unix socket. Intended for tests that
// drive the client against a fake JSON-RPC server (e.g. over net.Pipe), so
// call sites can be exercised without a running spdk_tgt.
func NewClientWithConn(ctx context.Context, conn net.Conn) *Client {
	return &Client{
		conn:    conn,
		jsonCli: jsonrpc.NewClient(ctx, conn),
	}
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

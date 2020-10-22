package cbrotli

import (
	"net"
	"sync"
	"time"

	brotli "github.com/google/brotli/go/cbrotli"
	"github.com/libp2p/go-libp2p-core/compression"
	"go.uber.org/multierr"
)

// ID is the protocol ID for noise
const ID = "/compression/cbrotli"

var _ compression.CompressedTransport = &Transport{}

type compConn struct {
	rlock sync.Mutex
	wlock sync.Mutex
	raw   net.Conn

	w *brotli.Writer
	r *brotli.Reader
}

// Transport defines a compression transport with a compression level.
type Transport struct {
	level int
}

// New Creates a new tranport with a specific compression level.
func New() *Transport {
	return &Transport{}
}

//NewConn upgrades a raw connection into a compressed connection.
func (t *Transport) NewConn(raw net.Conn, isServer bool) (compression.CompressedConn, error) {
	return &compConn{
		raw: raw,
		w:   brotli.NewWriter(raw, brotli.WriterOptions{Quality: 9}),
	}, nil
}

// Write compression wrapper
func (c *compConn) Write(b []byte) (int, error) {
	c.wlock.Lock()
	defer c.wlock.Unlock()
	n, err := c.w.Write(b)
	return n, multierr.Combine(err, c.w.Flush())
}

// Read compression wrapper
func (c *compConn) Read(b []byte) (int, error) {
	c.rlock.Lock()
	defer c.rlock.Unlock()
	if c.r == nil {
		// This _needs_ to be lazy as it reads a header.
		c.r = brotli.NewReader(c.raw)
	}
	n, err := c.r.Read(b)
	if err != nil {
		// It is important to close the reader to release resources.
		c.r.Close()
	}
	return n, err
}

func (c *compConn) Close() error {
	c.wlock.Lock()
	defer c.wlock.Unlock()
	return multierr.Combine(c.w.Close(), c.raw.Close())
}

func (c *compConn) LocalAddr() net.Addr {
	return c.raw.LocalAddr()
}

func (c *compConn) RemoteAddr() net.Addr {
	return c.raw.RemoteAddr()
}

func (c *compConn) SetDeadline(t time.Time) error {
	return c.raw.SetDeadline(t)
}

func (c *compConn) SetReadDeadline(t time.Time) error {
	return c.raw.SetReadDeadline(t)
}

func (c *compConn) SetWriteDeadline(t time.Time) error {
	return c.raw.SetWriteDeadline(t)
}

package toh

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/common/sched"
)

type ClientConn struct {
	idx      uint64
	tr       http.RoundTripper
	endpoint string
	failure  error

	write struct {
		counter uint64
		mu      sync.Mutex
		sched   sched.SchedKey
		buf     []byte
	}

	read *readConn
}

func Dial(network string, address string) (net.Conn, error) {
	c := NewClientConn("http://" + address)
	return c, nil
}

func NewClientConn(endpoint string) *ClientConn {
	c := &ClientConn{endpoint: endpoint}
	c.idx = rand.Uint64()
	c.tr = http.DefaultTransport
	c.read = newReadConn(c.idx)
	return c
}

func (c *ClientConn) intoFailureState(err error) {
	if err == nil {
		return
	}
	c.failure = err
}

func (c *ClientConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return nil
}

func (c *ClientConn) SetReadDeadline(t time.Time) error {
	c.read.ready.SetWaitDeadline(t)
	return nil
}

func (c *ClientConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (c *ClientConn) LocalAddr() net.Addr {
	return nil
}

func (c *ClientConn) RemoteAddr() net.Addr {
	return nil
}

func (c *ClientConn) Close() error {
	c.read.close()
	return nil
}

func (c *ClientConn) Write(p []byte) (n int, err error) {
	if c.failure != nil {
		return 0, c.failure
	}

	if c.read.closed {
		return 0, ErrClosedConn
	}

	c.write.mu.Lock()
	sched.Unschedule(c.write.sched)

	c.write.buf = append(c.write.buf, p...)
	if len(c.write.buf) < 1024 {
		// If there is no more writing requests, we schedule the task in 1 second
		// to force sending the buffer
		c.write.sched = sched.Schedule(func() {
			c.sendWriteBuf()
		}, time.Now().Add(time.Second))

		c.write.mu.Unlock()
		return len(p), nil
	}
	c.write.mu.Unlock()

	return len(p), c.sendWriteBuf()
}

func (c *ClientConn) sendWriteBuf() (err error) {
	c.write.mu.Lock()
	defer func() {
		if err != nil {
			c.intoFailureState(err)
			c.read.feedError(err)
		}
		c.write.mu.Unlock()
	}()

	if c.failure != nil {
		return c.failure
	}

	client := &http.Client{
		Transport: c.tr,
		//	Timeout:   c.write.deadline.Sub(time.Now()),
	}

	f := Frame{
		Idx:       atomic.AddUint64(&c.write.counter, 1),
		StreamIdx: c.idx,
		Data:      c.write.buf,
	}

	resp, err := client.Post(c.endpoint+"?s="+strconv.FormatUint(c.idx, 10), "application/octet-stream", f.Marshal())
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("remote is unavailable: %s", resp.Status)
	}

	c.write.buf = c.write.buf[:0]
	go func() {
		c.read.feedFrames(resp.Body)
		resp.Body.Close()
	}()

	return nil
}

func (c *ClientConn) Read(p []byte) (n int, err error) {
	return c.read.Read(p)
}

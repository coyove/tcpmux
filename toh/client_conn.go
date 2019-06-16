package toh

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/common/sched"
)

type ClientConn struct {
	idx      uint32
	tr       http.RoundTripper
	endpoint string

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

var globalConnCounter uint32 = 0

func NewClientConn(endpoint string) *ClientConn {
	c := &ClientConn{endpoint: endpoint}
	c.idx = atomic.AddUint32(&globalConnCounter, 1)
	c.tr = http.DefaultTransport
	c.read = newReadConn(c.idx, 'c')
	c.write.sched = sched.Schedule(c.schedSending, time.Now().Add(time.Second))
	return c
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
	return &net.TCPAddr{}
}

func (c *ClientConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *ClientConn) Close() error {
	c.write.sched.Cancel()
	c.read.close()
	return nil
}

func (c *ClientConn) Write(p []byte) (n int, err error) {
	if c.read.err != nil {
		return 0, c.read.err
	}

	if c.read.closed {
		return 0, ErrClosedConn
	}

	c.write.mu.Lock()
	c.write.sched.Cancel()
	c.write.sched = sched.Schedule(c.schedSending, time.Now().Add(time.Second))
	c.write.buf = append(c.write.buf, p...)
	c.write.mu.Unlock()

	if len(c.write.buf) < 128 {
		return len(p), nil
	}

	c.sendWriteBuf()
	return len(p), nil
}

func (c *ClientConn) schedSending() {
	c.sendWriteBuf()
	c.write.sched = sched.Schedule(c.schedSending, time.Now().Add(time.Second))
}

func (c *ClientConn) sendWriteBuf() {
	c.write.mu.Lock()
	defer c.write.mu.Unlock()

	if c.read.err != nil {
		return
	}

	client := &http.Client{
		Transport: c.tr,
		//	Timeout:   c.write.deadline.Sub(time.Now()),
	}

	f := Frame{
		Idx:     c.write.counter + 1,
		ConnIdx: c.idx,
		Data:    c.write.buf,
	}

	resp, err := client.Post(c.endpoint+"?s="+strconv.Itoa(int(c.idx)), "application/octet-stream", f.Marshal())
	if err != nil {
		c.read.feedError(err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		c.read.feedError(fmt.Errorf("remote is unavailable: %s", resp.Status))
		return
	}

	c.write.buf = c.write.buf[:0]
	c.write.counter++

	go func() {
		c.read.feedFrames(resp.Body)
		resp.Body.Close()
	}()
}

func (c *ClientConn) Read(p []byte) (n int, err error) {
	return c.read.Read(p)
}

func (c *ClientConn) String() string {
	return fmt.Sprintf("<ClientConn_%d_read_%v_write_%d>", c.idx, c.read, c.write.counter)
}

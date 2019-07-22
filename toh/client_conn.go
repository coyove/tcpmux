package toh

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/common/sched"
)

type ClientConn struct {
	idx    uint64
	dialer *Dialer

	write struct {
		sync.Mutex
		counter uint32
		sched   sched.SchedKey
		buf     []byte
		survey  struct {
			lastIsPositive bool
			pendingSize    int
			reschedCount   int64
		}
		respCh     chan io.ReadCloser
		respChOnce sync.Once
	}

	read *readConn
}

func (d *Dialer) Dial() (net.Conn, error) {
	return d.newClientConn()
}

func (d *Dialer) newClientConn() (net.Conn, error) {
	c := &ClientConn{dialer: d}
	c.idx = newConnectionIdx()
	c.write.survey.pendingSize = 1
	c.write.respCh = make(chan io.ReadCloser, 128)
	c.read = newReadConn(c.idx, d.blk, 'c')

	// Say hello
	resp, err := c.send(frame{
		idx:     rand.Uint32(),
		connIdx: c.idx,
		options: optSyncConnIdx,
		next: &frame{
			connIdx: c.idx,
			options: optHello,
		}})
	if err != nil {
		return nil, err
	}
	resp.Body.Close()

	c.write.sched = sched.Schedule(c.schedSending, time.Second)

	go c.respLoop()
	return c, nil
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
	vprint(c, " closing")
	c.write.sched.Cancel()
	c.read.close()
	c.write.respChOnce.Do(func() {
		close(c.write.respCh)
		go c.send(frame{
			connIdx: c.idx,
			options: optClosed,
		})
	})
	return nil
}

func (c *ClientConn) Write(p []byte) (n int, err error) {
REWRITE:
	if c.read.err != nil {
		return 0, c.read.err
	}

	if c.read.closed {
		return 0, errClosedConn
	}

	if len(c.write.buf) > MaxWriteBufferSize {
		vprint("write buffer is full")
		time.Sleep(time.Second)
		goto REWRITE
	}

	c.write.Lock()
	c.write.sched.Reschedule(func() {
		c.write.survey.pendingSize = 1
		c.schedSending()
	}, time.Second)
	c.write.buf = append(c.write.buf, p...)
	c.write.Unlock()

	if len(c.write.buf) < c.write.survey.pendingSize {
		return len(p), nil
	}

	c.schedSending()
	return len(p), nil
}

func (c *ClientConn) schedSending() {
	atomic.AddInt64(&c.write.survey.reschedCount, 1)

	if c.read.err != nil || c.read.closed {
		c.Close()
		return
	}

	c.dialer.orchSendWriteBuf(c)
	c.write.sched.Reschedule(func() {
		c.write.survey.pendingSize = 1
		c.schedSending()
	}, time.Second)
}

func (c *ClientConn) sendWriteBuf() {
	c.write.Lock()
	defer c.write.Unlock()

	if c.write.survey.pendingSize *= 2; c.write.survey.pendingSize > 1024 {
		c.write.survey.pendingSize = 1024
	}

	if c.read.err != nil {
		return
	}

	f := frame{
		idx:     rand.Uint32(),
		connIdx: c.idx,
		options: optSyncConnIdx,
		next: &frame{
			idx:     c.write.counter + 1,
			connIdx: c.idx,
			data:    c.write.buf,
		},
	}

	deadline := time.Now().Add(c.dialer.Timeout - time.Second)
	for {
		if resp, err := c.send(f); err != nil {
			if time.Now().After(deadline) {
				c.read.feedError(err)
				return
			}
		} else {
			c.write.buf = c.write.buf[:0]
			c.write.counter++
			func() {
				defer func() { recover() }()
				select {
				case c.write.respCh <- resp.Body:
				default:
					go func(resp *http.Response) {
						c.read.feedframes(resp.Body)
						resp.Body.Close()
					}(resp)
				}
			}()
			break
		}
	}
}

func (c *ClientConn) send(f frame) (resp *http.Response, err error) {
	client := &http.Client{
		Timeout:   c.dialer.Timeout,
		Transport: c.dialer.Transport,
	}

	req, _ := http.NewRequest("POST", "http://"+c.dialer.endpoint, f.marshal(c.read.blk))
	resp, err = client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("remote is unavailable: %s", resp.Status)
	}
	return resp, nil
}

func (c *ClientConn) respLoop() {
	for body := range c.write.respCh {
		k := sched.Schedule(func() { body.Close() }, c.dialer.Timeout)
		if n, _ := c.read.feedframes(body); n == 0 {
			c.write.survey.lastIsPositive = false
		}
		k.Cancel()
		body.Close()
	}
	vprint(c, " resp out")
}

func (c *ClientConn) Read(p []byte) (n int, err error) {
	return c.read.Read(p)
}

func (c *ClientConn) String() string {
	return fmt.Sprintf("<C:%x,r:%d,w:%d>", c.idx, c.read.counter, c.write.counter)
}

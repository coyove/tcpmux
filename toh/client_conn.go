package toh

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/common/sched"
)

var DefaultTransport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext,
	MaxIdleConns:          1,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

type ClientConn struct {
	idx      uint32
	endpoint string

	write struct {
		sync.Mutex
		counter uint64
		sched   sched.SchedKey
		buf     []byte
		survey  struct {
			pendingSize  int
			reschedCount int64
		}
	}

	read *readConn
}

func Dial(network string, address string) (net.Conn, error) {
	blk, _ := aes.NewCipher([]byte(network + "0123456789abcdef")[:16])
	c, err := newClientConn("http://"+address, blk)
	if err != nil {
		return nil, err
	}
	return c, nil
}

var globalConnCounter uint32 = 0

func newClientConn(endpoint string, blk cipher.Block) (*ClientConn, error) {
	c := &ClientConn{endpoint: endpoint}
	c.idx = atomic.AddUint32(&globalConnCounter, 1)
	c.write.sched = sched.Schedule(c.schedSending, time.Now().Add(time.Second))
	c.write.survey.pendingSize = 64
	c.read = newReadConn(c.idx, blk, 'c')

	// Say hello
	client := &http.Client{Transport: DefaultTransport}
	f := Frame{ConnIdx: c.idx, Options: OptHello, Data: []byte{}}
	req, _ := http.NewRequest("POST", c.endpoint, f.Marshal(c.read.blk))
	req.Header.Add("ETag", connIdxToString(c.read.blk, c.idx))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote is unavailable: %s", resp.Status)
	}
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

	c.write.Lock()
	c.write.sched.Cancel()
	c.write.sched = sched.Schedule(func() {
		c.write.survey.pendingSize = 1
		c.schedSending()
	}, time.Now().Add(time.Second))
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
	c.sendWriteBuf()
	c.write.sched = sched.Schedule(func() {
		if c.read.err != nil || c.read.closed {
			vprint(c, " schedSending out")
			return
		}
		c.write.survey.pendingSize = 1
		c.schedSending()
	}, time.Now().Add(time.Second))
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

	client := &http.Client{Transport: DefaultTransport}

	f := Frame{
		Idx:     c.write.counter + 1,
		ConnIdx: c.idx,
		Data:    c.write.buf,
	}

	req, _ := http.NewRequest("POST", c.endpoint, f.Marshal(c.read.blk))
	req.Header.Add("ETag", connIdxToString(c.read.blk, c.idx))
	resp, err := client.Do(req)
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

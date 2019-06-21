package toh

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
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

var (
	DefaultTransport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	MaxGetSize         = 1024
	InactivePurge      = time.Minute
	ClientReadTimeout  = time.Second * 15
	MaxWriteBufferSize = 1024 * 1024 * 2
	ErrBigWriteBuf     = fmt.Errorf("writer size exceeds limit, reader may be dead")
	OnRequestServer    = func() *http.Transport { return DefaultTransport }

	globalConnCounter uint32 = 0
)

type respNode struct {
	r io.ReadCloser
	f frame
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
		respCh     chan respNode
		respChOnce sync.Once
	}

	read *readConn
}

func Dial(network string, address string) (net.Conn, error) {
	blk, _ := aes.NewCipher([]byte(network + "0123456789abcdef")[:16])
	c, err := newClientConn("http://"+address+"/", blk)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func newClientConn(endpoint string, blk cipher.Block) (*ClientConn, error) {
	c := &ClientConn{endpoint: endpoint}
	c.idx = atomic.AddUint32(&globalConnCounter, 1)
	c.write.survey.pendingSize = 1
	c.write.respCh = make(chan respNode, 16)
	c.read = newReadConn(c.idx, blk, 'c')

	// Say hello
	resp, err := c.send(frame{
		idx:     rand.Uint64(),
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

	c.write.sched = sched.ScheduleSync(c.schedSending, time.Now().Add(time.Second))

	go c.respLoop()
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
	c.write.respChOnce.Do(func() { close(c.write.respCh) })
	return nil
}

func (c *ClientConn) Write(p []byte) (n int, err error) {
	if c.read.err != nil {
		return 0, c.read.err
	}

	if c.read.closed {
		return 0, ErrClosedConn
	}

	if len(c.write.buf) > MaxWriteBufferSize {
		return 0, ErrBigWriteBuf
	}

	c.write.Lock()
	c.write.sched.RescheduleSync(func() {
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

	if c.read.err != nil || c.read.closed {
		c.Close()
		return
	}

	orchSendWriteBuf(c)
	c.write.sched.RescheduleSync(func() {
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

	f := frame{
		idx:     rand.Uint64(),
		connIdx: c.idx,
		options: optSyncConnIdx,
		next: &frame{
			idx:     c.write.counter + 1,
			connIdx: c.idx,
			data:    c.write.buf,
		},
	}

	deadline := time.Now().Add(InactivePurge - time.Second)
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
				c.write.respCh <- respNode{r: resp.Body}
			}()
			break
		}
	}
}

func (c *ClientConn) send(f frame) (resp *http.Response, err error) {
	client := &http.Client{
		Timeout:   time.Second * 5,
		Transport: OnRequestServer(),
	}

	if f.size() >= MaxGetSize {
		req, _ := http.NewRequest("POST", c.endpoint, f.marshal(c.read.blk))
		resp, err = client.Do(req)
	} else {
		url := &bytes.Buffer{}
		url.WriteString(c.endpoint)
		enc := base64.NewEncoder(base64.URLEncoding, url)
		io.Copy(enc, f.marshal(c.read.blk))
		enc.Close() // flush padding
		resp, err = client.Get(url.String())
	}
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
	for {
		select {
		case body, ok := <-c.write.respCh:
			if !ok {
				return
			}
			if body.r != nil {
				k := sched.ScheduleSync(func() { body.r.Close() }, time.Now().Add(ClientReadTimeout))
				c.read.feedframes(body.r)
				k.Cancel()
				body.r.Close()
			} else {
				if c.read.err == nil && !c.read.closed {
					c.read.feedframe(body.f)
				}
			}
		} // end of select
	}
}

func (c *ClientConn) Read(p []byte) (n int, err error) {
	return c.read.Read(p)
}

func (c *ClientConn) String() string {
	return fmt.Sprintf("<ClientConn_%d_read_%v_write_%d>", c.idx, c.read, c.write.counter)
}

package toh

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
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
		MaxIdleConns:          1,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	InactivePurge      = time.Minute
	MaxWriteBufferSize = 1024 * 1024 * 2
	MaxMissingSize     = 1024 * 1024 * 4
	ErrBigWriteBuf     = fmt.Errorf("writer size exceeds limit, reader may be dead")
	OnRequestServer    = func() *http.Transport { return DefaultTransport }

	globalConnCounter uint32 = 0
)

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
		failedframes chan frame
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

func newClientConn(endpoint string, blk cipher.Block) (*ClientConn, error) {
	c := &ClientConn{endpoint: endpoint}
	c.idx = atomic.AddUint32(&globalConnCounter, 1)
	c.write.sched = sched.Schedule(c.schedSending, time.Now().Add(time.Second))
	c.write.survey.pendingSize = 64
	c.write.failedframes = make(chan frame, 1024)
	c.read = newReadConn(c.idx, blk, 'c')

	// Say hello
	client := &http.Client{Transport: DefaultTransport}
	f := frame{connIdx: c.idx, options: optHello, data: []byte{}}
	req, _ := http.NewRequest("POST", c.endpoint, f.marshal(c.read.blk))
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

	if len(c.write.buf) > MaxWriteBufferSize {
		return 0, ErrBigWriteBuf
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

	if c.read.err != nil || c.read.closed {
		//	vprint(c, " schedSending out")
		return
	}

	c.sendWriteBuf()
	c.write.sched = sched.Schedule(func() {
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

	client := &http.Client{
		Timeout:   InactivePurge * 2,
		Transport: OnRequestServer(),
	}

	var f frame
AGAIN:
	select {
	case f = <-c.write.failedframes:
		if len(f.data) == 0 {
			goto AGAIN
		}
		if f.fails++; f.fails > 16 {
			c.read.feedError(fmt.Errorf("remote is unavailable"))
			return
		}
		vprint(c, " retry failed frame: ", f)
	default:
		f = frame{
			idx:     c.write.counter + 1,
			connIdx: c.idx,
			data:    make([]byte, len(c.write.buf)),
			next: &frame{
				idx:     c.read.counter,
				options: optSyncIdx,
				data:    []byte{},
			},
		}
		copy(f.data, c.write.buf)
		c.write.buf = c.write.buf[:0]
		c.write.counter++
	}

	go func() {
		if randomFailures && rand.Intn(6) == 0 {
			c.write.failedframes <- f
			return
		}

		req, _ := http.NewRequest("POST", c.endpoint, f.marshal(c.read.blk))
		req.Header.Add("ETag", connIdxToString(c.read.blk, c.idx))
		resp, err := client.Do(req)
		if err != nil {
			//c.read.feedError(err)
			c.write.failedframes <- f
			return
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			//c.read.feedError(fmt.Errorf("remote is unavailable: %s", resp.Status))
			c.write.failedframes <- f
			return
		}

		c.read.feedframes(resp.Body)
		resp.Body.Close()
	}()
}

func (c *ClientConn) Read(p []byte) (n int, err error) {
	return c.read.Read(p)
}

func (c *ClientConn) String() string {
	return fmt.Sprintf("<ClientConn_%d_read_%v_write_%d>", c.idx, c.read, c.write.counter)
}

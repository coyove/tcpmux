package toh

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/coyove/common/waitobject"
)

var (
	ErrClosedConn = fmt.Errorf("use of closed connection")
	dummyTouch    = func(interface{}) interface{} { return 1 }
)

type readConn struct {
	idx          uint64
	counter      uint64
	mu           sync.Mutex
	buf          []byte
	frames       chan Frame
	futureFrames map[uint64]Frame
	ready        *waitobject.Object
	err          error
	closed       bool
}

func newReadConn(idx uint64) *readConn {
	r := &readConn{
		frames:       make(chan Frame, 1024),
		futureFrames: map[uint64]Frame{},
		idx:          idx,
	}
	r.ready = waitobject.New()
	go r.readLoopRearrange()
	return r
}

func (c *readConn) feedFrames(r io.Reader) {
	for {
		f, ok := ParseFrame(r)
		if !ok {
			break
		}
		debugprint("feed: ", f.Data)
		c.frames <- f
	}
}

func (c *readConn) feedError(err error) {
	c.err = err
	c.ready.Touch(dummyTouch)
}

func (c *readConn) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.frames)
	c.ready.SetWaitDeadline(time.Now())
}

func (c *readConn) readLoopRearrange() {
	for {
		select {
		case f, ok := <-c.frames:
			if !ok {
				return
			}

			c.mu.Lock()
			if f.StreamIdx != c.idx {
				c.mu.Unlock()
				c.feedError(fmt.Errorf("fatal: unmatched stream index"))
				return
			}
			c.futureFrames[f.Idx] = f
			for {
				if f := c.futureFrames[c.counter+1]; f.StreamIdx == c.idx {
					c.buf = append(c.buf, f.Data...)
					c.counter = f.Idx
					delete(c.futureFrames, f.Idx)
				} else {
					break
				}
			}
			c.mu.Unlock()
			c.ready.Touch(dummyTouch)
		}
	}
}

func (c *readConn) Read(p []byte) (n int, err error) {
READ:
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return 0, ErrClosedConn
	}

	if c.err != nil {
		c.mu.Unlock()
		return 0, c.err
	}

	if len(c.buf) > 0 {
		n = copy(p, c.buf)
		c.buf = c.buf[n:]
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	_, ontime := c.ready.Wait()

	if c.closed {
		c.mu.Unlock()
		return 0, ErrClosedConn
	}

	if !ontime {
		return 0, fmt.Errorf("timeout")
	}

	if c.err != nil {
		return 0, c.err
	}

	goto READ
}

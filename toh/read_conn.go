package toh

import (
	"crypto/cipher"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/coyove/common/waitobject"
)

var (
	ErrClosedConn = fmt.Errorf("use of closed connection")
	dummyTouch    = func(interface{}) interface{} { return 1 }
)

type readConn struct {
	counter       uint64
	mu            sync.Mutex
	buf           []byte
	frames        chan Frame
	futureFrames  map[uint64]Frame
	missingFrames map[uint64]uint64
	ready         *waitobject.Object
	err           error
	blk           cipher.Block
	closed        bool
	tag           byte
	idx           uint32
}

func newReadConn(idx uint32, blk cipher.Block, tag byte) *readConn {
	r := &readConn{
		frames:        make(chan Frame, 1024),
		futureFrames:  map[uint64]Frame{},
		missingFrames: map[uint64]uint64{},
		idx:           idx,
		tag:           tag,
		blk:           blk,
	}
	r.ready = waitobject.New()
	go r.readLoopRearrange()
	return r
}

func (c *readConn) feedFrames(r io.Reader) (datalen int, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Dirty way to avoid closed channel panic
			if strings.Contains(fmt.Sprintf("%v", r), "send on close") {
				datalen = 0
				err = ErrClosedConn
			} else {
				panic(r)
			}
		}
	}()

	count := 0
	for {
		f, ok := ParseFrame(r, c.blk)
		if !ok {
			return 0, fmt.Errorf("invalid frames")
		}
		if f.Idx == 0 {
			break
		}
		if c.closed {
			return 0, ErrClosedConn
		}
		if c.err != nil {
			return 0, c.err
		}

		debugprint("feed: ", string(f.Data))
		c.frames <- f
		count += len(f.Data)
	}
	return count, nil
}

func (c *readConn) feedError(err error) {
	c.err = err
	c.ready.Touch(dummyTouch)
	// readLoop will still continue
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
			if f.ConnIdx != c.idx {
				c.mu.Unlock()
				c.feedError(fmt.Errorf("fatal: unmatched stream index"))
				return
			}

			if f.Idx <= c.counter {
				c.mu.Unlock()
				c.feedError(fmt.Errorf("unmatched counter, maybe server GCed the connection"))
				return
			}

			c.futureFrames[f.Idx] = f
			for {
				idx := c.counter + 1
				if f, ok := c.futureFrames[idx]; ok {
					c.buf = append(c.buf, f.Data...)
					c.counter = f.Idx
					delete(c.futureFrames, f.Idx)
					delete(c.missingFrames, f.Idx)
				} else {
					c.missingFrames[idx]++

					if x := c.missingFrames[idx]; x > 16 {
						c.mu.Unlock()
						c.feedError(fmt.Errorf("fatal: missing certain frame"))
						vprint("missings: ", c.missingFrames, ", futures: ", c.futureFrames)
						return
					} else if x > 2 {
						vprint("temp missing: ", idx, ", tries: ", x)
					}
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
	if c.closed {
		return 0, ErrClosedConn
	}

	if c.err != nil {
		return 0, c.err
	}

	if c.ready.IsTimedout() {
		return 0, fmt.Errorf("timeout")
	}

	c.mu.Lock()
	if len(c.buf) > 0 {
		n = copy(p, c.buf)
		c.buf = c.buf[n:]
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	_, ontime := c.ready.Wait()

	if c.closed {
		return 0, ErrClosedConn
	}

	if !ontime {
		return 0, fmt.Errorf("timeout")
	}

	goto READ
}

func (c *readConn) String() string {
	return fmt.Sprintf("<readConn_%d_%s_ctr_%d>", c.idx, string(c.tag), c.counter)
}

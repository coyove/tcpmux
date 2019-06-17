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
	sync.Mutex
	counter       uint64             // counter, must be synced with the writer on the other side
	buf           []byte             // read buffer
	frames        chan frame         // incoming frames
	futureframes  map[uint64]frame   // future frames, which have arrived early
	missingframes map[uint64]uint64  // missing frames, readConn is waiting for their arrivals
	ready         *waitobject.Object // it being touched means that data in "buf" are ready
	err           error              // stored error, if presented, all operations afterwards should return it
	blk           cipher.Block       // cipher block, aes-128
	closed        bool               // is readConn closed already
	tag           byte               // tag, 'c' for readConn in ClientConn, 's' for readConn in ServerConn
	idx           uint32             // readConn index, should be the same as the one in ClientConn/SerevrConn
}

func newReadConn(idx uint32, blk cipher.Block, tag byte) *readConn {
	r := &readConn{
		frames:        make(chan frame, 1024),
		futureframes:  map[uint64]frame{},
		missingframes: map[uint64]uint64{},
		//sentframes:    lru.NewCache(WriteCacheSize),
		idx:   idx,
		tag:   tag,
		blk:   blk,
		ready: waitobject.New(),
	}
	go r.readLoopRearrange()
	return r
}

func (c *readConn) feedframes(r io.Reader) (datalen int, err error) {
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
		f, ok := parseframe(r, c.blk)
		if !ok {
			err = fmt.Errorf("invalid frames")
			c.feedError(err)
			return 0, err
		}
		if f.idx == 0 {
			break
		}
		if c.closed {
			return 0, ErrClosedConn
		}
		if c.err != nil {
			return 0, c.err
		}
		if f.options&optSyncIdx > 0 {
			continue
		}

		debugprint("feed: ", f.data)
		c.frames <- f
		count += len(f.data)
	}
	return count, nil
}

func (c *readConn) feedError(err error) {
	c.err = err
	c.ready.Touch(dummyTouch)
	c.close()
}

func (c *readConn) close() {
	c.Lock()
	defer c.Unlock()
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
		//		case <-time.After(time.Second * 10):
		//			vprint("timeout")
		case f, ok := <-c.frames:
			if !ok {
				return
			}

			c.Lock()
			if f.connIdx != c.idx {
				c.Unlock()
				c.feedError(fmt.Errorf("fatal: unmatched stream index"))
				return
			}

			if f.idx <= c.counter {
				c.Unlock()
				//c.feedError(fmt.Errorf("unmatched counter, maybe server GCed the connection"))
				return
			}

			c.futureframes[f.idx] = f
			for {
				idx := c.counter + 1
				if f, ok := c.futureframes[idx]; ok {
					c.buf = append(c.buf, f.data...)
					c.counter = f.idx
					delete(c.futureframes, f.idx)
					delete(c.missingframes, f.idx)
				} else {
					c.missingframes[idx]++

					if x := c.missingframes[idx]; x > 16 {
						c.Unlock()
						c.feedError(fmt.Errorf("fatal: missing certain frame"))
						vprint("missings: ", c.missingframes, ", futures: ", c.futureframes)
						return
					} else if x > 2 {
						vprint("temp missing: ", idx, ", tries: ", x)
					}
					break
				}
			}
			c.Unlock()
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

	c.Lock()
	if len(c.buf) > 0 {
		n = copy(p, c.buf)
		c.buf = c.buf[n:]
		c.Unlock()
		return
	}
	c.Unlock()

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

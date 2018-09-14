package tcpmux

import (
	"io"
	"net"
	"sync"
	"time"
)

type state struct {
	buf []byte
	n   int
	err error
	idx uint32
	cmd byte
}

type notify struct {
	f   byte
	err error
}

type Stream struct {
	master       *connState
	readTmp      []byte
	read         chan *state
	readmu       sync.Mutex
	readState    chan notify
	writeState   chan notify
	streamIdx    uint32
	lastActive   uint32
	closed       bool
	remoteClosed bool
	tag          byte
	options      byte
	timeout      uint32
	rdeadline    int64
	wdeadline    int64
}

func timeNow() uint32 {
	return uint32(time.Now().Unix())
}

func newStream(id uint32, c *connState) *Stream {
	s := &Stream{
		streamIdx:  id,
		master:     c,
		read:       make(chan *state, readRespChanSize),
		writeState: make(chan notify, 1),
		readState:  make(chan notify, 1),
		lastActive: timeNow(),
	}
	return s
}

func (c *Stream) handleStateNotify(x byte) (bool, error) {
	switch {
	case isset(x, notifyClose):
		c.closed = true
		return false, ErrConnClosed
	case isset(x, notifyRemoteClosed):
		c.remoteClosed = true
		return false, io.EOF
	case isset(x, notifyCancel):
		return true, nil
	}
	return false, nil
}

func (c *Stream) Read(buf []byte) (n int, err error) {
	if c.closed {
		return 0, ErrConnClosed
	}

	if c.remoteClosed {
		return 0, io.EOF
	}

	if _, err := c.handleStateNotify(c.getStateNonBlock(c.readState)); err != nil {
		return 0, err
	}

	var after time.Duration
	if c.rdeadline > 0 {
		after = time.Duration(c.rdeadline-time.Now().UnixNano()/1e6) * time.Millisecond
		if int64(after) < 0 {
			return 0, &timeoutError{}
		}
	} else {
		after = time.Second
	}

	c.lastActive = timeNow()

	c.readmu.Lock()
	defer c.readmu.Unlock()

	if c.readTmp != nil {
		copy(buf, c.readTmp)

		if len(c.readTmp) > len(buf) {
			c.readTmp = c.readTmp[len(buf):]
			return len(buf), nil
		}

		n = len(c.readTmp)
		c.readTmp = nil
		return
	}

REPEAT:
	select {
	case x := <-c.readState:
		if cancel, err := c.handleStateNotify(x); err != nil {
			return 0, err
		} else if cancel {
			return 0, &timeoutError{}
		}
	case x := <-c.read:
		switch x.cmd {
		case cmdAck:
			// remote has acknowledged this stream
			// repeat reading for new messages
			goto REPEAT
		}

		if x.err != nil {
			return 0, x.err
		}

		n, err = x.n, x.err
		if x.buf != nil {
			xbuf := x.buf[:x.n]
			if len(xbuf) > len(buf) {
				c.readTmp = xbuf[len(buf):]
				n = len(buf)
			}
			copy(buf, xbuf)
		}
	case <-time.After(after):
		if c.rdeadline == 0 {
			goto REPEAT
		}
		return 0, &timeoutError{}
	}

	c.lastActive = timeNow()
	return
}

func (c *Stream) Write(buf []byte) (n int, err error) {
	if c.closed {
		return 0, ErrConnClosed
	}

	if c.remoteClosed {
		return 0, io.EOF
	}

	if len(buf) > bufferSize {
		return 0, ErrLargeWrite
	}

	if _, err := c.handleStateNotify(c.getStateNonBlock(c.writeState)); err != nil {
		return 0, err
	}

	var after time.Duration
	if c.wdeadline > 0 {
		after = time.Duration(c.wdeadline-time.Now().UnixNano()/1e6) * time.Millisecond
		if int64(after) < 0 {
			return 0, &timeoutError{}
		}
	} else {
		after = time.Second
	}

	writeOK := make(chan bool, 1)
	go func() {
		n, err = c.master.conn.Write(makeFrame(c.streamIdx, 0, buf))
		writeOK <- true
	}()

REPEAT:
	select {
	case <-writeOK:
		if err != nil {
			return 0, err
		}
	case x := <-c.writeState:
		if cancel, err := c.handleStateNotify(x); err != nil {
			return 0, err
		} else if cancel {
			return 0, &timeoutError{}
		}
	case <-time.After(after):
		if c.wdeadline == 0 {
			goto REPEAT
		}
		return 0, &timeoutError{}
	}

	c.lastActive = timeNow()
	n -= 8
	return
}

func (c *Stream) notifyRead(code byte) {
	select {
	case c.readExit <- code:
	default:
	}
}

func isset(b notify, flag byte) bool { return (b.f & flag) > 0 }

func (c *Stream) getStateNonBlock(ch chan notify) (s notify) {
	select {
	case s = <-ch:
		c.sendStateNonBlock(ch, s)
	default:
	}
	return
}

func (c *Stream) sendStateNonBlock(ch chan notify, s notify) {
	select {
	case x := <-ch:
		select {
		case ch <- x | s:
		default:
		}
	default:
		select {
		case ch <- s:
		default:
		}
	}
}

func (c *Stream) closeNoInfo() {
	c.closed = true
	c.sendStateNonBlock(c.writeState, notify{f: notifyClose})
	c.sendStateNonBlock(c.readState, notify{f: notifyClose})
}

// Close closes the stream and remove it from its master
func (c *Stream) Close() error {
	c.closeNoInfo()
	// logg.D(buf)
	if _, err := c.master.conn.Write(makeFrame(c.streamIdx, cmdClose, nil)); err != nil {
		c.master.broadcast(err)
	}

	c.master.streams.Delete(c.streamIdx)
	return nil
}

// CloseMaster closes all streams under the same master net.Conn
func (c *Stream) CloseMaster() error {
	c.master.stop()
	return nil
}

// LocalAddr is a compatible method for net.Conn
func (c *Stream) LocalAddr() net.Addr { return c.master.conn.LocalAddr() }

// RemoteAddr is a compatible method for net.Conn
func (c *Stream) RemoteAddr() net.Addr { return c.master.conn.RemoteAddr() }

// SetReadDeadline is a compatible method for net.Conn
func (c *Stream) SetReadDeadline(t time.Time) error {
	// Usually a time.Time{} is used for clearing the deadline
	if t.IsZero() {
		c.rdeadline = 0
		return nil
	}

	if t.UnixNano() < time.Now().UnixNano()+1e6 {
		c.rdeadline = t.UnixNano()/1e6 - 1
		c.sendStateNonBlock(c.readState, notifyCancel)
		return nil
	}

	c.rdeadline = t.UnixNano() / 1e6
	return nil
}

// SetWriteDeadline is a compatible method for net.Conn
func (c *Stream) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		c.wdeadline = 0
		return nil
	}

	if t.UnixNano() < time.Now().UnixNano()+1e6 {
		c.wdeadline = t.UnixNano()/1e6 - 1
		c.sendStateNonBlock(c.writeState, notifyCancel)
		return nil
	}

	c.wdeadline = t.UnixNano()/1e6 - 1
	return nil
}

// SetDeadline is a compatible method for net.Conn
func (c *Stream) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	c.SetWriteDeadline(t)
	return nil
}

func (c *Stream) SetInactiveTimeout(secs uint32) {
	c.timeout = secs
}

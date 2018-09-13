package tcpmux

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type readState struct {
	buf []byte
	n   int
	err error
	idx uint32
	cmd byte
}

type Stream struct {
	rm, wm sync.Mutex
	master *connState

	streamIdx uint32

	lastResp        *readState
	readResp        chan *readState
	readOverflowBuf []byte

	writeStateResp chan byte // state returned from remote when writing

	closed    atomic.Value
	readExit  chan byte // inform Read() to exit or timeout
	writeExit chan byte // inform Write() to exit or timeout

	tag     byte // for debug purpose
	options byte

	lastActive int64
	timeout    int64
}

func newStream(id uint32, c *connState) *Stream {
	s := &Stream{
		streamIdx:      id,
		master:         c,
		readExit:       make(chan byte, 1),
		writeExit:      make(chan byte, 1),
		readResp:       make(chan *readState, readRespChanSize),
		writeStateResp: make(chan byte, 1),
		timeout:        streamTimeout,
		lastActive:     time.Now().UnixNano(),
	}

	s.closed.Store(false)
	return s
}

func (c *Stream) SetStreamOpt(opt byte) {
	c.options |= opt
}

func (c *Stream) notifyCodeError(code byte) error {
	switch code {
	case notifyExit:
		if (c.options & OptErrWhenClosed) > 0 {
			return ErrConnClosed
		}
		return io.EOF
	case notifyCancel:
		return &timeoutError{}
	default:
		panic("unknown error")
	}
}

func (c *Stream) Read(buf []byte) (n int, err error) {
	c.rm.Lock()
	defer c.rm.Unlock()

	if c == nil || c.closed.Load().(bool) {
		return 0, c.notifyCodeError(notifyExit)
	}

	if c.readOverflowBuf != nil {
		copy(buf, c.readOverflowBuf)

		if len(c.readOverflowBuf) > len(buf) {
			c.readOverflowBuf = c.readOverflowBuf[len(buf):]
			return len(buf), nil
		}

		n = len(c.readOverflowBuf)
		c.readOverflowBuf = nil
		return
	}

	c.lastActive = time.Now().UnixNano()

REPEAT:
	select {
	case x := <-c.readResp:
		if x.cmd == cmdAck {
			goto REPEAT
		}

		if x.cmd == cmdClose {
			if (c.options & OptErrWhenClosed) > 0 {
				return 0, ErrConnClosed
			}
			return 0, io.EOF
		}

		n, err = x.n, x.err

		if x.buf != nil {
			xbuf := x.buf[:x.n]
			if len(xbuf) > len(buf) {
				c.readOverflowBuf = xbuf[len(buf):]
				n = len(buf)
			}

			copy(buf, xbuf)
		}

		if x.err != nil {
			n = 0
		}

		c.lastResp = x
	case code := <-c.readExit:
		return 0, c.notifyCodeError(code)
	}

	c.lastActive = time.Now().UnixNano()
	return
}

// Write is NOT a thread-safe function, it is intended to be used only in one goroutine
func (c *Stream) Write(buf []byte) (n int, err error) {
	c.wm.Lock()
	defer c.wm.Unlock()

	if c == nil || c.closed.Load().(bool) {
		if (c.options & OptErrWhenClosed) > 0 {
			return 0, ErrConnClosed
		}
		return len(buf), nil
	}

	if len(buf) > bufferSize {
		return 0, fmt.Errorf("the buffer is larger than %d, try splitting it into smaller ones", bufferSize)
	}

	writeChan := make(chan bool, 1)
	go func() {
		n, err = c.master.conn.Write(makeFrame(c.streamIdx, 0, buf))
		writeChan <- true
	}()

	select {
	case <-writeChan:
		if err != nil {
			return
		}
	case code := <-c.writeExit:
		return 0, c.notifyCodeError(code)
	}

	c.lastActive = time.Now().UnixNano()
	select {
	case cmd := <-c.writeStateResp:
		switch cmd {
		case cmdErr:
			return 0, errors.New("write: remote returns an error")
		case cmdClose:
			if (c.options & OptErrWhenClosed) > 0 {
				return 0, ErrConnClosed
			}
			return len(buf), nil //ErrConnClosed
		}
	case code := <-c.writeExit:
		return 0, c.notifyCodeError(code)
	default:
	}

	c.lastActive = time.Now().UnixNano()
	n -= 7
	return
}

func (c *Stream) notifyRead(code byte) {
	select {
	case c.readExit <- code:
	default:
	}
}

func (c *Stream) notifyWrite(code byte) {
	select {
	case c.writeExit <- code:
	default:
	}
}

func (c *Stream) closeNoInfo() {
	c.notifyRead(notifyExit)
	c.notifyWrite(notifyExit)
	c.closed.Store(true)
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

// SetTimeout sets the timeout for this stream, error range: 1 sec
func (c *Stream) SetTimeout(sec int64) {
	c.timeout = sec
	c.master.timeout = sec
}

// SetMasterTimeout sets the timeout for this stream's net.Conn, error range: 1 sec
func (c *Stream) SetMasterTimeout(sec int64) {
	c.master.timeout = sec
}

// LocalAddr is a compatible method for net.Conn
func (c *Stream) LocalAddr() net.Addr { return c.master.conn.LocalAddr() }

// RemoteAddr is a compatible method for net.Conn
func (c *Stream) RemoteAddr() net.Addr { return c.master.conn.RemoteAddr() }

// SetReadDeadline is a compatible method for net.Conn
func (c *Stream) SetReadDeadline(t time.Time) error {
	// Usually a time.Time{} is used for clearing the deadline
	// But we must have an internal timeout, so ignore it
	if t.IsZero() {
		clearCancel(c.readExit)
		return nil
	}

	if t.UnixNano() < time.Now().UnixNano()+1e6 {
		c.notifyRead(notifyCancel)
		return nil
	}

	return nil
}

// SetWriteDeadline is a compatible method for net.Conn
func (c *Stream) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		clearCancel(c.writeExit)
		return nil
	}

	if t.UnixNano() < time.Now().UnixNano()+1e6 {
		c.notifyWrite(notifyCancel)
		return nil
	}

	return nil
}

// SetDeadline is a compatible method for net.Conn
func (c *Stream) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	c.SetWriteDeadline(t)
	return nil
}

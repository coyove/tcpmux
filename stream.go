package tcpmux

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/coyove/common/waitobject"
)

type notify struct {
	err  error
	idx  uint32
	ack  bool
	flag byte
	src  byte
}

type Stream struct {
	master       *connState
	readbuf      []byte
	readmu       sync.Mutex
	writemu      sync.Mutex
	read         *waitobject.Object
	write        *waitobject.Object
	streamIdx    uint32
	lastActive   uint32
	closed       bool
	remoteClosed bool
	tag          byte
	timeout      uint32
}

func timeNow() uint32 {
	return uint32(time.Now().Unix())
}

func newStream(id uint32, c *connState) *Stream {
	s := &Stream{
		streamIdx:  id,
		master:     c,
		write:      waitobject.New(),
		read:       waitobject.New(),
		readbuf:    make([]byte, 0),
		lastActive: timeNow(),
	}
	s.timeout = c.timeout
	return s
}

func (c *Stream) Read(buf []byte) (n int, err error) {
	c.lastActive = timeNow()

READ:
	c.readmu.Lock()
	if len(c.readbuf) > 0 {
		n = copy(buf, c.readbuf)
		c.readbuf = c.readbuf[n:]
		c.readmu.Unlock()
		return
	}
	c.readmu.Unlock()

	if c.closed {
		return 0, ErrConnClosed
	}
	if c.remoteClosed {
		return 0, io.EOF
	}

	v, ontime := c.read.Wait()
	if !ontime {
		// a timeout signal may be triggerred by:
		//  1. a real timedout event
		//  2. someone canceled/closed the object explicitly
		return 0, &timeoutError{}
	}

	switch x := v.(notify); x.flag {
	case notifyReady:
		// data is ready, read them
		goto READ
	case notifyRemoteClosed:
		c.remoteClosed = true
		return 0, io.EOF
	case notifyClose:
		c.closed = true
		return 0, ErrConnClosed
	case notifyCancel:
		return 0, &timeoutError{}
	case notifyError:
		return 0, x.err
	default:
		panic("shouldn't happen")
	}
}

func (c *Stream) Write(buf []byte) (n int, err error) {
	c.lastActive = timeNow()

	c.writemu.Lock()
	defer c.writemu.Unlock()

	if c.closed {
		return 0, ErrConnClosed
	}
	if c.remoteClosed {
		return len(buf), nil
	}

	c.master.writeQueue <- writePending{
		data: c.master.makeFrame(c.streamIdx, cmdPayload, c.tag == 'c', buf),
		obj:  c.write,
	}

	v, ontime := c.write.Wait()
	if !ontime {
		// a timeout signal may be triggerred by:
		//  1. a real timedout event
		//  2. someone canceled/closed the object explicitly
		return 0, &timeoutError{}
	}

	switch x := v.(notify); x.flag {
	case notifyReady:
		// data is sent already
		n = len(buf)
		return
	case notifyRemoteClosed:
		c.remoteClosed = true
		return 0, io.EOF
	case notifyClose:
		c.closed = true
		return 0, ErrConnClosed
	case notifyCancel:
		return 0, &timeoutError{}
	case notifyError:
		return 0, x.err
	default:
		panic("shouldn't happen")
	}
}

func (c *Stream) String() string {
	return fmt.Sprintf("<stream%d_%s>", c.streamIdx, string(c.tag))
}

func (c *Stream) closeNoInfo() {
	debugprint(c, ", touching before closing")

	n := notify{flag: notifyClose, src: 'c'}
	c.closed = true
	c.write.Touch(n)
	c.read.Touch(n)
}

// Close closes the stream and remove it from its master
func (c *Stream) Close() error {
	c.closeNoInfo()

	if _, err := c.master.conn.Write(c.master.makeFrame(c.streamIdx, cmdRemoteClosed, c.tag == 'c', nil)); err != nil {
		c.master.broadcastErrAndStop(err)
	}

	debugprint(c, ", closing")
	c.master.streams.Delete(c.streamIdx)
	return nil
}

// LocalAddr is a compatible method for net.Conn
func (c *Stream) LocalAddr() net.Addr { return c.master.conn.LocalAddr() }

// RemoteAddr is a compatible method for net.Conn
func (c *Stream) RemoteAddr() net.Addr { return c.master.conn.RemoteAddr() }

// SetReadDeadline is a compatible method for net.Conn
func (c *Stream) SetReadDeadline(t time.Time) error {
	c.read.SetWaitDeadline(t)
	return nil
}

// SetWriteDeadline is a compatible method for net.Conn
func (c *Stream) SetWriteDeadline(t time.Time) error {
	c.write.SetWaitDeadline(t)
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

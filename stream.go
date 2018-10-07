package tcpmux

import (
	"io"
	"net"
	"sync"
	"time"
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
	read         chan notify
	write        chan notify
	streamIdx    uint32
	lastActive   uint32
	closed       bool
	remoteClosed bool
	tag          byte
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
		write:      make(chan notify, 1),
		read:       make(chan notify, 1),
		readbuf:    make([]byte, 0),
		lastActive: timeNow(),
	}

	s.timeout = c.timeout
	return s
}

func (c *Stream) Read(buf []byte) (n int, err error) {
	c.lastActive = timeNow()

	var after time.Duration
	if c.rdeadline > 0 {
		after = time.Duration(c.rdeadline-time.Now().UnixNano()/1e6) * time.Millisecond
		if int64(after) < 0 {
			return 0, &timeoutError{}
		}
	} else {
		after = time.Second
	}

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
	// log.Println("read", c.streamIdx)
REPEAT:
	select {
	case x := <-c.read:
		if isset(x, notifyReady) {
			// We are notified that the data is ready
			// But we will still check the remaining notifications:
			//   for RemoteClosed, we continue, when the data has all been read, we return io.EOF
			//   for Close, we do the same as above, but return ErrConnClosed
			//   for Cancel, we immediately return timeout error, the data remains in the buffer
			switch {
			case isset(x, notifyRemoteClosed):
				c.remoteClosed = true
			case isset(x, notifyClose):
				c.closed = true
			case isset(x, notifyCancel):
				return 0, &timeoutError{}
			}
			goto READ
		}
		switch {
		case isset(x, notifyRemoteClosed):
			c.remoteClosed = true
			return 0, io.EOF
		case isset(x, notifyCancel):
			return 0, &timeoutError{}
		case isset(x, notifyError):
			return 0, x.err
		case isset(x, notifyClose):
			c.closed = true
			return 0, ErrConnClosed
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
	var after time.Duration
	if c.wdeadline > 0 {
		after = time.Duration(c.wdeadline-time.Now().UnixNano()/1e6) * time.Millisecond
		if int64(after) < 0 {
			return 0, &timeoutError{}
		}
	} else {
		after = time.Second
	}

	c.lastActive = timeNow()

	if c.closed {
		return 0, ErrConnClosed
	}

	if c.remoteClosed {
		return len(buf), nil
	}

	if len(buf) > bufferSize {
		return 0, ErrLargeWrite
	}

	go func() {
		n, err = c.master.writeFrame(c.streamIdx, 0, c.tag == 'c', buf)
		// log.Println("->", c.streamIdx)
		c.sendStateNonBlock(c.write, notify{flag: notifyReady})
	}()

	// log.Println("Wait", c.streamIdx)
	// log.Println("Write", c.streamIdx, string(buf))
REPEAT:
	select {
	case x := <-c.write:
		if isset(x, notifyReady) {
			// conn.Write is completed
		}
		switch {
		case isset(x, notifyRemoteClosed):
			c.remoteClosed = true
			return len(buf), nil
		case isset(x, notifyCancel):
			return 0, &timeoutError{}
		case isset(x, notifyError):
			return 0, x.err
		case isset(x, notifyClose):
			c.closed = true
			return 0, ErrConnClosed
		}
	case <-time.After(after):
		if c.wdeadline == 0 {
			goto REPEAT
		}
		return 0, &timeoutError{}
	}

	// log.Println("Wait OK", c.streamIdx)
	c.lastActive = timeNow()
	n = len(buf)
	return
}

func isset(b notify, flag byte) bool { return (b.flag & flag) > 0 }

func (c *Stream) sendStateNonBlock(ch chan notify, s notify) {
	select {
	case x := <-ch:
		x.flag |= s.flag
		x.err = s.err
		select {
		case ch <- x:
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
	c.sendStateNonBlock(c.write, notify{flag: notifyClose, src: 'c'})
	c.sendStateNonBlock(c.read, notify{flag: notifyClose, src: 'c'})
}

// Close closes the stream and remove it from its master
func (c *Stream) Close() error {
	c.closeNoInfo()
	// logg.D(buf)
	if _, err := c.master.conn.Write(c.master.makeFrame(c.streamIdx, cmdRemoteClosed, c.tag == 'c', nil)); err != nil {
		c.master.broadcast(err)
	}

	// x := make([]byte, 32*1024)
	// // n := runtime.Stack(x, false)
	// n := 0
	// log.Println("close", time.Now().UnixNano()/1e3, c.streamIdx, string(c.tag), string(x[:n]))
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
		c.rdeadline = 0
		c.sendStateNonBlock(c.read, notify{flag: notifyCancel, src: 'r'})
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
		c.wdeadline = 0
		c.sendStateNonBlock(c.write, notify{flag: notifyCancel, src: 'w'})
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

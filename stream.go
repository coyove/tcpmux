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
	flag notifyFlag
}

type Stream struct {
	master     *connState
	readbuf    []byte
	readmu     sync.Mutex
	writemu    sync.Mutex
	read       *waitobject.Object
	write      *waitobject.Object
	streamIdx  uint32
	lastActive uint32
	tag        byte
	timeout    uint32
}

func timeNow() uint32 {
	return uint32(time.Now().Unix())
}

func touch(obj *waitobject.Object, n notify) {
	obj.Touch(func(o interface{}) interface{} {
		if o == nil {
			return n
		}

		no := o.(notify)
		if no.err == nil {
			no.err = n.err
		}
		old := no.flag
		no.flag |= n.flag
		debugprint("touch, old: ", old, ", new: ", no.flag)
		return no
	})
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

	if c.read.IsTimedout() {
		return 0, &timeoutError{}
	}

READ:
	c.readmu.Lock()
	if len(c.readbuf) > 0 {
		n = copy(buf, c.readbuf)
		c.readbuf = c.readbuf[n:]
		c.readmu.Unlock()
		debugprint(c, " finished reading: ", n, " - ", string(buf[:n]))
		return
	}
	c.readmu.Unlock()

	oldv := c.read.SetValue(nil)
	if x, _ := oldv.(notify); x.flag > 0 {
		if x.flag&notifyClose > 0 {
			return 0, io.EOF
		}
		if x.flag&notifyError > 0 {
			return 0, x.err
		}
	}

	debugprint(c, " waits reading")
	v, ontime := c.read.Wait()
	if !ontime {
		// a timeout signal may be triggerred by:
		//  1. a real timedout event
		//  2. someone canceled/closed the object explicitly
		debugprint(c, " waitobject timeout")
		return 0, &timeoutError{}
	}

	switch x := v.(notify); {
	case x.flag&notifyReady > 0:
		// data is ready, read them
		c.read.SetValue(func(v interface{}) interface{} {
			x.flag ^= notifyReady
			return x
		})
		goto READ
	case x.flag&notifyClose > 0:
		return 0, io.EOF
	case x.flag&notifyCancel > 0:
		return 0, &timeoutError{}
	case x.flag&notifyError > 0:
		return 0, x.err
	case x.flag&notifyAck > 0:
		// Continux waiting
		goto READ
	default:
		panic(byte(x.flag))
	}
}

func (c *Stream) Write(buf []byte) (n int, err error) {
	c.lastActive = timeNow()

	if c.write.IsTimedout() {
		return 0, &timeoutError{}
	}

	oldv := c.read.SetValue(nil)
	if x, _ := oldv.(notify); x.flag > 0 {
		if x.flag&notifyClose > 0 {
			return 0, io.EOF
		}
		if x.flag&notifyError > 0 {
			return 0, x.err
		}
	}

	c.writemu.Lock()
	defer c.writemu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			if fmt.Sprintf("%v", r) == "send on closed channel" {
				n, err = 0, io.EOF
			} else {
				panic(r)
			}
		}
	}()

	c.master.writeQueue <- writePending{
		data: c.master.makeFrame(c.streamIdx, cmdPayload, c.tag == 'c', buf),
		obj:  c.write,
	}

	debugprint(c, " waits writing")
	v, ontime := c.write.Wait(func(v interface{}) waitobject.WaitReturn {
		if n, _ := v.(notify); n.flag <= notifyClose && n.flag > 0 {
			// Close or Error
			return waitobject.DoNotWait
		}
		return waitobject.Wait
	})
	if !ontime {
		// a timeout signal may be triggerred by:
		//  1. a real timedout event
		//  2. someone canceled/closed the object explicitly
		return 0, &timeoutError{}
	}

	debugprint(c, " waits writing finished")
	switch x := v.(notify); {
	case x.flag&notifyReady > 0:
		// data is sent already
		debugprint(c, " waits writing finished: ", string(buf))
		c.write.SetValue(func(v interface{}) interface{} {
			n := v.(notify)
			n.flag &= ^notifyReady
			return n
		})
		n = len(buf)
		return
	case x.flag&notifyClose > 0:
		return 0, io.EOF
	case x.flag&notifyCancel > 0:
		return 0, &timeoutError{}
	case x.flag&notifyError > 0:
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

	n := notify{flag: notifyClose}
	touch(c.write, n)
	touch(c.read, n)
}

// Close closes the stream and remove it from its master
func (c *Stream) Close() error {
	debugprint(c, ", closing")
	c.closeNoInfo()
	c.master.streams.Delete(c.streamIdx)

	frame := c.master.makeFrame(c.streamIdx, cmdRemoteClosed, c.tag == 'c', nil)
	if _, err := c.master.conn.Write(frame); err != nil {
		c.master.broadcastErrAndStop(err)
		return err
	}

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

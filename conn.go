package tcpmux

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"unsafe"

	"github.com/coyove/common/waitobject"
)

type writePending struct {
	data []byte
	obj  *waitobject.Object
}

type connState struct {
	conn     net.Conn
	master   Map32
	streams  Map32
	idx      uint32
	timeout  uint32
	exitRead chan bool
	key      []byte

	newStreamCallback func(state notify)
	Sum32             func([]byte, []byte) uint32
	ErrorCallback     func(error) bool

	stopped bool
	tag     byte
	sync.Mutex

	writeQueue chan writePending
	exitWrite  chan bool
}

// When something serious happened, we broadcast it to every stream and close the master conn
// TCP connections may have temporary errors, but here we treat them as the same as other failures
func (cs *connState) broadcastErrAndStop(err error) {
	if cs.ErrorCallback != nil {
		cs.ErrorCallback(err)
	}

	n := notify{flag: notifyError, err: err}
	cs.streams.Iterate(func(idx uint32, s unsafe.Pointer) bool {
		c := (*Stream)(s)
		c.read.Touch(n)
		c.write.Touch(n)
		return true
	})

	cs.Lock()
	defer cs.Unlock()

	if cs.stopped {
		return
	}

	cs.streams.Iterate(func(idx uint32, p unsafe.Pointer) bool {
		s := (*Stream)(p)
		s.closeNoInfo()
		return true
	})

	cs.exitWrite <- true
	cs.conn.Close()
	cs.master.Delete(cs.idx)
	cs.stopped = true
}

func (cs *connState) start() {
	debugprint(cs, " started")
	go cs.readLoop()
	go cs.writeLoop()
}

func (cs *connState) String() string {
	return fmt.Sprintf("<connState%d_%s>", cs.idx, string(cs.tag))
}

func (cs *connState) readLoop() {
	for {
		payload, n, err := WSRead(cs.conn)
		if err != nil {
			cs.broadcastErrAndStop(err)
			return
		}

		hash := binary.BigEndian.Uint32(payload[:4])
		streamIdx := binary.BigEndian.Uint32(payload[4:])

		if hash != cs.Sum32(payload[4:], cs.key) {
			// If we found a invalid hash, the whole connection wll not be stable anymore
			cs.broadcastErrAndStop(ErrInvalidHash)
			return
		}

		// it's a control frame
		if n == 9 && payload[8] != cmdPayload {
			switch payload[8] {
			case cmdHello:
				// The stream will be added into connState in this callback
				cs.newStreamCallback(notify{idx: streamIdx})

				// We acknowledge the hello
				if _, err = cs.conn.Write(cs.makeFrame(streamIdx, cmdAck, false, nil)); err != nil {
					cs.broadcastErrAndStop(err)
					return
				}

				debugprint(cs, ", stream: ", streamIdx, ", received hello")
			case cmdAck:
				if p, ok := cs.streams.Load(streamIdx); ok {
					((*Stream)(p)).read.Touch(notify{flag: notifyAck})
				}
				debugprint(cs, ", stream: ", streamIdx, ", received ack")
			case cmdRemoteClosed:
				if p, ok := cs.streams.Load(streamIdx); ok {
					s := (*Stream)(p)
					// log.Println("receive remote close", string(s.tag), s.streamIdx)
					// Inform the stream that its remote has closed itself
					n := notify{flag: notifyRemoteClosed, src: 'm'}
					s.read.Touch(n)
					s.write.Touch(n)
				}
				debugprint(cs, ", stream: ", streamIdx, ", remote stream has been closed")
			default:
				cs.broadcastErrAndStop(fmt.Errorf("unknown remote command: %d", payload[7]))
				return
			}
			continue
		}

		if s, ok := cs.streams.Load(streamIdx); ok {
			c := (*Stream)(s)
			c.readmu.Lock()
			c.readbuf = append(c.readbuf, payload[9:]...)
			c.readmu.Unlock()
			c.read.Touch(notify{flag: notifyReady})
			debugprint(cs, ", stream: ", streamIdx, ", read ready: ", payload[9:])
		} else {
			if _, err = cs.conn.Write(cs.makeFrame(streamIdx, cmdRemoteClosed, false, nil)); err != nil {
				cs.broadcastErrAndStop(err)
				return
			}
			debugprint(cs, ", stream: ", streamIdx, ", this is closed, you should close as well")
		}
	}
}

func (cs *connState) writeLoop() {
	for {
		select {
		case wp := <-cs.writeQueue:
			_, err := cs.conn.Write(wp.data)
			if err != nil {
				cs.broadcastErrAndStop(err)
				return
			}
			wp.obj.Touch(notify{flag: notifyReady})
		case <-cs.exitWrite:
			return
		}
	}
}

// Conn can prefetch one byte from net.Conn before Read()
type Conn struct {
	first     bool
	firstdata [1]byte
	firsterr  error

	net.Conn
}

func (c *Conn) FirstByte() (b byte, err error) {
	if c.first {
		return c.firstdata[0], c.firsterr
	}

	c.first = true
	var n int

	n, err = c.Conn.Read(c.firstdata[:])
	c.firsterr = err

	if n == 1 {
		b = c.firstdata[0]
	}

	return
}

func (c *Conn) Read(p []byte) (int, error) {
	if c.firsterr != nil {
		return 0, c.firsterr
	}

	if c.first {
		p[0] = c.firstdata[0]
		xp := p[1:]

		n, err := c.Conn.Read(xp)
		c.first = false

		return n + 1, err
	}

	return c.Conn.Read(p)
}

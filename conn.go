package tcpmux

import (
	"encoding/binary"
	"fmt"
	"net"
	"unsafe"

	sync "github.com/sasha-s/go-deadlock"

	"github.com/coyove/common/waitobject"
)

type writePending struct {
	data []byte
	obj  *waitobject.Object
}

type connState struct {
	conn    net.Conn
	master  Map32
	streams Map32
	idx     uint32
	timeout uint32
	key     []byte

	newStreamCallback func(steamIdx uint32)
	Sum32             func([]byte, []byte) uint32
	ErrorCallback     func(error) bool

	stopper sync.Once
	tag     byte

	writeQueue chan writePending
}

// When something serious happened, we broadcast it to every stream and close the master conn
// TCP connections may have temporary errors, but here we treat them as the same as other failures
func (cs *connState) broadcastErrAndStop(err error) {
	cs.stopper.Do(func() {
		// After conn closed, readLoop and writeLoop will encounter errors, and exit
		cs.conn.Close()
		close(cs.writeQueue)
		cs.master.Delete(cs.idx)

		n := notify{flag: notifyError, err: err}
		cs.streams.Iterate(func(idx uint32, s unsafe.Pointer) bool {
			c := (*Stream)(s)
			touch(c.read, n)
			touch(c.write, n)
			return true
		})
	})
}

func (cs *connState) start() {
	debugprint(cs, " started")
	go cs.readLoop()
	go cs.writeLoop()
}

func (cs *connState) String() string {
	return fmt.Sprintf("<connState_%d_%s>", cs.idx, string(cs.tag))
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
			// If we found an invalid hash, the whole connection will not be stable anymore
			cs.broadcastErrAndStop(ErrInvalidHash)
			return
		}

		// it's a control frame
		if n == 9 && payload[8] != cmdPayload {
			switch payload[8] {
			case cmdHello:
				// The stream will be added into connState in this callback
				cs.newStreamCallback(streamIdx)

				// We acknowledge the hello
				if _, err = cs.conn.Write(cs.makeFrame(streamIdx, cmdAck, false, nil)); err != nil {
					cs.broadcastErrAndStop(err)
					return
				}

				debugprint(cs, ", stream: ", streamIdx, ", received hello")
			case cmdAck:
				if p, ok := cs.streams.Load(streamIdx); ok {
					touch(((*Stream)(p)).read, notify{flag: notifyAck})
				}
				debugprint(cs, ", stream: ", streamIdx, ", received ack")
			case cmdRemoteClosed:
				if p, ok := cs.streams.Fetch(streamIdx); ok {
					s := (*Stream)(p)
					s.Close()
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
			debugprint(cs, ", stream: ", streamIdx, ", read ready: ", n, " - ", string(c.readbuf))
			c.readmu.Unlock()
			touch(c.read, notify{flag: notifyReady})
		} else {
			if _, err = cs.conn.Write(cs.makeFrame(streamIdx, cmdRemoteClosed, false, nil)); err != nil {
				cs.broadcastErrAndStop(err)
				return
			}
			debugprint(cs, ", stream: ", streamIdx, ", this is closed, you should close as well")
		}
	}
}

// writeLoop only write raw bytes to TCP conns, that means
// the incoming payloads in writeQueue are "frames" already
func (cs *connState) writeLoop() {
	for {
		select {
		case wp, ok := <-cs.writeQueue:
			if !ok {
				return
			}
			_, err := cs.conn.Write(wp.data)
			if err != nil {
				cs.broadcastErrAndStop(err)
				return
			}
			touch(wp.obj, notify{flag: notifyReady})
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

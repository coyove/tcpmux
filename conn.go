package tcpmux

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
	"unsafe"
)

type connState struct {
	conn net.Conn

	master  Map32
	streams Map32

	idx uint32

	exitRead chan bool

	newStreamCallback func(state *state)
	ErrorCallback     func(error) bool

	timeout int64
	stopped bool
	sync.Mutex
}

// When something serious happened, we broadcast it to every stream and close the master conn
// TCP connections may have temporary errors, but here we treat them as the same as other failures
func (cs *connState) broadcast(err error) {
	if cs.ErrorCallback != nil {
		cs.ErrorCallback(err)
	}

	cs.streams.Iterate(func(idx uint32, s unsafe.Pointer) bool {
		(*Stream)(s).readResp <- &readState{err: err}
		return true
	})

	cs.stop()
}

func (cs *connState) start() {
	readChan, daemonExit := make(chan bool), make(chan bool)

	go func() {
		for {
			time.Sleep(time.Second)

			select {
			case <-daemonExit:
				return
			default:
				now := uint32(time.Now().UnixNano() / 1e9)

				// Garbage collect all closed and/or inactive streams
				cs.streams.Iterate(func(idx uint32, p unsafe.Pointer) bool {
					s := (*Stream)(p)
					if s.closed {
						// return false to delete
						return false
					}

					// TODO
					if to := s.timeout; to > 0 && (now-s.lastActive)/1e9 <= to {
						return true
					}

					s.sendStateNonBlock(s.readState, notifyCancel)
					s.sendStateNonBlock(s.writeState, notifyCancel)
					return false
				})
			}
		}
	}()

	for {
		go func() {
			buf := [8]byte{}

			// cs.conn.SetReadDeadline(time.Now().Add(time.Duration(cs.timeout) * time.Second))
			_, err := io.ReadAtLeast(cs.conn, buf[:], 8)

			if err != nil {
				cs.broadcast(err)
				return
			}

			streamIdx := binary.BigEndian.Uint32(buf[2:])
			streamLen := int(binary.BigEndian.Uint16(buf[6:]))

			if buf[5] == cmdByte && buf[6] != 0 {
				switch buf[6] {
				case cmdHello:
					// The stream will be added into connState in this callback
					cs.newStreamCallback(&readState{idx: streamIdx})

					buf[5], buf[6] = cmdByte, cmdAck
					// We acknowledge the hello
					if _, err = cs.conn.Write(buf); err != nil {
						cs.broadcast(err)
						return
					}

					fallthrough
				case cmdPing:
					readChan <- true
					return
				default:
					if p, ok := cs.streams.Load(streamIdx); ok {
						s, cmd := (*Stream)(p), buf[6]
						select {
						case s.writeStateResp <- cmd:
						default:
						}

						select {
						case s.readResp <- &readState{cmd: cmd}:
						default:
						}
					}
				}

				readChan <- true
				return
			}

			payload := make([]byte, streamLen)
			_, err = io.ReadAtLeast(cs.conn, payload, streamLen)
			// Maybe we will encounter an error, but we pass it to streams
			// Next loop when we read the header, we will have the error again, that time we will broadcast
			rs := &readState{
				n:   streamLen,
				err: err,
				buf: payload,
				idx: streamIdx,
			}

			if s, ok := cs.streams.Load(streamIdx); ok {
				(*Stream)(s).readResp <- rs
			} else {
				buf[5], buf[6] = cmdByte, cmdClose
				if _, err = cs.conn.Write(buf); err != nil {
					cs.broadcast(err)
					return
				}
			}
			readChan <- true
		}()

		select {
		case <-cs.exitRead:
			daemonChan <- true
			return
		default:
		}

		select {
		case <-readChan:
		case <-cs.exitRead:
			daemonChan <- true
			return
		}
	}
}

// Even all streams are closed, the conn will still not be removed from the master.
// It gets removed only if it encountered an error, and stop() was called, or any one of its streams called CloseMaster()
func (cs *connState) stop() {
	cs.Lock()
	if cs.stopped {
		cs.Unlock()
		return
	}

	cs.exitRead <- true
	cs.streams.Iterate(func(idx uint32, p unsafe.Pointer) bool {
		s := (*Stream)(p)
		s.closeNoInfo()
		return true
	})

	cs.conn.Close()
	cs.master.Delete(cs.idx)

	cs.stopped = true
	cs.Unlock()
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

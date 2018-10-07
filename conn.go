package tcpmux

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
	"unsafe"
)

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
	sync.Mutex
}

// When something serious happened, we broadcast it to every stream and close the master conn
// TCP connections may have temporary errors, but here we treat them as the same as other failures
func (cs *connState) broadcast(err error) {
	if cs.ErrorCallback != nil {
		cs.ErrorCallback(err)
	}

	cs.streams.Iterate(func(idx uint32, s unsafe.Pointer) bool {
		c := (*Stream)(s)
		c.sendStateNonBlock(c.read, notify{flag: notifyError, err: err})
		c.sendStateNonBlock(c.write, notify{flag: notifyError, err: err})
		return true
	})

	cs.stop()
}

func (cs *connState) writeFrame(idx uint32, cmd byte, mask bool, payload []byte) (int, error) {
	return cs.conn.Write(cs.makeFrame(idx, cmd, mask, payload))
}

func (cs *connState) start() {
	readChan, daemonExit := make(chan bool), make(chan bool)

	// start daemon, it will do the gc work
	go func() {
		for {
			time.Sleep(time.Second)

			select {
			case <-daemonExit:
				return
			default:
				now := uint32(time.Now().Unix())
				// Garbage collect all closed and/or inactive streams
				cs.streams.Iterate(func(idx uint32, p unsafe.Pointer) bool {
					s := (*Stream)(p)
					if s.closed {
						// return false to delete
						return false
					}

					// TODO
					if to := s.timeout; to == 0 || now-s.lastActive <= to {
						return true
					}

					s.sendStateNonBlock(s.read, notify{flag: notifyClose, src: 'g'})
					s.sendStateNonBlock(s.write, notify{flag: notifyClose, src: 'g'})
					return false
				})
			}
		}
	}()

	for {
		go func() {
			payload, n, err := WSRead(cs.conn)
			if err != nil {
				cs.broadcast(err)
				return
			}

			hash := binary.BigEndian.Uint32(payload[:4])
			xhash := cs.Sum32(payload[4:], cs.key)
			streamIdx := binary.BigEndian.Uint32(payload[4:])

			// it's a control frame
			if n == 9 && payload[8] != cmdPayload {
				if hash != xhash {
					// if we found a invalid hash, then the whole connection is not stable any more
					// broadcast this error and stop all
					log.Println("invalid hash:", hash, xhash, payload)
					cs.broadcast(ErrInvalidHash)
					return
				}

				switch payload[8] {
				case cmdHello:
					// The stream will be added into connState in this callback
					cs.newStreamCallback(notify{idx: streamIdx})

					// We acknowledge the hello
					if _, err = cs.writeFrame(streamIdx, cmdAck, false, nil); err != nil {
						cs.broadcast(err)
						return
					}
				case cmdAck:
					if p, ok := cs.streams.Load(streamIdx); ok {
						s := (*Stream)(p)
						s.read <- notify{ack: true}
					}
				case cmdRemoteClosed:
					if p, ok := cs.streams.Load(streamIdx); ok {
						s := (*Stream)(p)
						// log.Println("receive remote close", string(s.tag), s.streamIdx)
						s.sendStateNonBlock(s.write, notify{flag: notifyRemoteClosed, src: 'm'})
						s.sendStateNonBlock(s.read, notify{flag: notifyRemoteClosed, src: 'm'})
					}
				default:
					cs.broadcast(fmt.Errorf("unknown remote command: %d", payload[7]))
				}

				readChan <- true
				return
			}

			if hash != xhash {
				// if we found a invalid hash, then the whole connection is not stable any more
				// broadcast this error and stop all
				log.Println("invalid hash:", hash, xhash, payload)
				cs.broadcast(ErrInvalidHash)
				return
			}
			// Maybe we will encounter an error, but we pass it to streams
			// Next loop when we read the header, we will have the error again, that time we will broadcast
			rs := notify{
				err:  err,
				idx:  streamIdx,
				flag: notifyReady,
			}

			if s, ok := cs.streams.Load(streamIdx); ok {
				c := (*Stream)(s)
				c.readmu.Lock()
				c.readbuf = append(c.readbuf, payload[9:]...)
				c.readmu.Unlock()
				c.sendStateNonBlock(c.read, rs)
			} else {
				if _, err = cs.writeFrame(streamIdx, cmdRemoteClosed, false, nil); err != nil {
					cs.broadcast(err)
					return
				}
			}
			readChan <- true
		}()

		select {
		case <-cs.exitRead:
			daemonExit <- true
			return
		default:
		}

		select {
		case <-readChan:
			// continue next loop
		case <-cs.exitRead:
			daemonExit <- true
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

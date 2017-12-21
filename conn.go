package tcpmux

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type connState struct {
	address *net.TCPAddr

	master  Map32
	streams Map32

	idx uint32

	conn     net.Conn
	exitRead chan bool

	newStreamCallback func(state *readState)
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
		(*Stream)(s).readResp <- &readState{err: err}
		return true
	})

	cs.stop()
}

func (cs *connState) start() {
	readChan, daemonChan := make(chan bool), make(chan bool)

	go func() {
		for {
			time.Sleep(2 * time.Second)

			select {
			case <-daemonChan:
				return
			default:
				now := time.Now().UnixNano()
				cs.streams.Iterate(func(idx uint32, p unsafe.Pointer) bool {
					s := (*Stream)(p)
					if s.closed.Load().(bool) {
						// return false to delete
						return false
					}

					// TODO
					if to := atomic.LoadInt64(&s.timeout); to > 0 && (now-s.lastActive)/1e9 <= to {
						return true
					}

					s.notifyRead(notifyCancel)
					s.notifyWrite(notifyCancel)
					return false
				})
				//logg.D(">>>>>>", cs.streams.Len())
				if _, err := cs.conn.Write(makeFrame(0, cmdPing, nil)); err != nil {
					cs.broadcast(err)
					return
				}
			}
		}
	}()

	for {
		go func() {

			buf := make([]byte, 7)
			_, err := io.ReadAtLeast(cs.conn, buf, 7)
			if err != nil {
				cs.broadcast(err)
				return
			}

			if buf[0] != version {
				cs.broadcast(errors.New("fatal: invalid header received"))
				return
			}

			streamIdx := binary.BigEndian.Uint32(buf[1:])
			streamLen := int(binary.BigEndian.Uint16(buf[5:]))

			if buf[5] == cmdByte {
				switch buf[6] {
				case cmdHello:
					cs.newStreamCallback(&readState{idx: streamIdx})

					buf[5], buf[6] = cmdByte, cmdAck
					// we acknowledge the hello
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
					// if it is an invalid index, we do nothing to prevent infinite loop, next time the sender writes,
					// he won't receive any confirmation, so he knows the receiver is dead
				}

				readChan <- true
				return
			}

			payload := make([]byte, streamLen)
			_, err = io.ReadAtLeast(cs.conn, payload, streamLen)
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
// It gets removed only if it encountered an error, and stop() was called.
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

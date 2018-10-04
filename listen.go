package tcpmux

import (
	"errors"
	"log"
	"net"
	"sync/atomic"
	"time"
	"unsafe"
)

type ListenPool struct {
	ln net.Listener

	realConns Map32
	conns     Map32
	streams   Map32

	connsCtr  uint32
	streamCtr uint32

	newStreamWaiting chan uint64

	exit      chan bool
	exitA     chan bool
	acceptErr chan error

	ErrorCallback func(error) bool
}

func Listen(addr string, pooling bool) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	if !pooling {
		return ln, err
	}

	return Wrap(ln), nil
}

func Wrap(ln net.Listener) net.Listener {
	lp := &ListenPool{
		ln:        ln,
		exit:      make(chan bool, 1),
		exitA:     make(chan bool, 1),
		acceptErr: make(chan error, 1),
		conns:     Map32{}.New(),
		streams:   Map32{}.New(),
		realConns: Map32{}.New(),

		newStreamWaiting: make(chan uint64, acceptStreamChanSize),
	}

	go lp.accept()
	return lp
}

func (l *ListenPool) Upgrade(conn net.Conn) {
	var c *connState
	counter := atomic.AddUint32(&l.connsCtr, 1)

	c = &connState{
		ErrorCallback: l.ErrorCallback,
		idx:           counter,
		conn:          conn,
		master:        l.conns,
		exitRead:      make(chan bool),
		timeout:       streamTimeout,
		streams:       Map32{}.New(),
		newStreamCallback: func(state notify) {
			idx := state.idx
			s := newStream(idx, c)
			s.tag = 's'

			c.streams.Store(idx, s)
			l.streams.Store(idx, s)
			l.newStreamWaiting <- uint64(idx)
		},
	}

	l.conns.Store(counter, c)
	go c.start()
}

func (l *ListenPool) accept() {
ACCEPT:
	for {
		select {
		case <-l.exit:
			return
		default:
			_conn, err := l.ln.Accept()
			if err != nil {
				l.acceptErr <- err
				return
			}

			conn := &Conn{Conn: _conn}

			conn.SetReadDeadline(time.Now().Add(pingInterval * time.Second))
			ver, err := conn.FirstByte()
			conn.SetReadDeadline(time.Time{})
			if err != nil {
				continue ACCEPT
			}

			if ver != 2 {
				newCtr := atomic.AddUint32(&l.connsCtr, 1)
				l.realConns.Store(newCtr, conn)
				idx := uint64(newCtr) << 32
				if len(l.newStreamWaiting) == acceptStreamChanSize {
					log.Println("tcpmux: approach the channel buffer limit")
				}
				l.newStreamWaiting <- idx
				continue ACCEPT
			}

			l.Upgrade(conn)
		}
	}
}

func (l *ListenPool) Accept() (net.Conn, error) {
	select {
	case idx := <-l.newStreamWaiting:
		if idx&0xffffffff00000000 > 0 {
			c, ok := l.realConns.Fetch(uint32(idx >> 32))
			if !ok {
				return nil, errors.New("fatal: conn lost")
			}
			return (*Conn)(c), nil
		}

		s, ok := l.streams.Fetch(uint32(idx))
		if !ok {
			return nil, errors.New("fatal: stream lost")
		}
		return (*Stream)(s), nil
	case <-l.exitA:
		return nil, errors.New("accept: listener has ended")
	case err := <-l.acceptErr:
		return nil, err
	}
}

func (l *ListenPool) Close() error {
	l.exit <- true
	l.exitA <- true
	return l.ln.Close()
}

func (l *ListenPool) Addr() net.Addr {
	return l.ln.Addr()
}

func (l *ListenPool) Count() (int, int, []int) {
	conns := make([]int, 0, l.conns.Len())

	l.conns.IterateConst(func(id uint32, p unsafe.Pointer) bool {
		conns = append(conns, (*connState)(p).streams.Len())
		return true
	})

	return len(l.newStreamWaiting), l.streams.Len(), conns
}

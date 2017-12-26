package tcpmux

import (
	"errors"
	"net"
	"time"
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

			l.connsCtr++

			if ver != Version {
				l.realConns.Store(l.connsCtr, conn)
				idx := uint64(l.connsCtr) << 32
				l.newStreamWaiting <- idx
				continue ACCEPT
			}

			var c *connState
			c = &connState{
				ErrorCallback: l.ErrorCallback,
				idx:           l.connsCtr,
				conn:          conn,
				master:        l.conns,
				exitRead:      make(chan bool),
				timeout:       streamTimeout,
				streams:       Map32{}.New(),
				newStreamCallback: func(state *readState) {
					idx := state.idx
					s := newStream(idx, c)
					s.tag = 's'

					c.streams.Store(idx, s)
					l.streams.Store(idx, s)
					l.newStreamWaiting <- uint64(idx)
				},
			}

			l.conns.Store(l.connsCtr, c)
			go c.start()
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

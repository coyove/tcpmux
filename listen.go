package tcpmux

import (
	"errors"
	"net"
)

type ListenPool struct {
	ln net.Listener

	conns   Map32
	streams Map32

	connsCtr  uint32
	streamCtr uint32

	newStreamWaiting chan uint32

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

		newStreamWaiting: make(chan uint32, acceptStreamChanSize),
	}

	go lp.accept()
	return lp
}

func (l *ListenPool) accept() {
	for {
		select {
		case <-l.exit:
			return
		default:
			conn, err := l.ln.Accept()
			if err != nil {
				l.acceptErr <- err
				return
			}

			l.connsCtr++
			var c *connState
			c = &connState{
				ErrorCallback: l.ErrorCallback,
				idx:           l.connsCtr,
				conn:          conn,
				master:        l.conns,
				exitRead:      make(chan bool),
				streams:       Map32{}.New(),
				newStreamCallback: func(state *readState) {
					idx := state.idx
					s := newStream(idx, c)
					s.tag = 's'

					c.streams.Store(idx, s)
					l.streams.Store(idx, s)
					l.newStreamWaiting <- idx
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
		s, ok := l.streams.Fetch(idx)
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

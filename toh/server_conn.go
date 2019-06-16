package toh

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/coyove/common/sched"
)

type ServerConn struct {
	idx        uint32
	rev        *Listener
	counter    uint64
	schedPurge sched.SchedKey
	blk        cipher.Block

	write struct {
		mu      sync.Mutex
		buf     []byte
		counter uint64
	}

	read *readConn
}

type Listener struct {
	ln           net.Listener
	closed       bool
	conns        map[uint32]*ServerConn
	connsmu      sync.Mutex
	httpServeErr chan error
	pendingConns chan *ServerConn
	blk          cipher.Block

	InactivePurge time.Duration
}

func (l *Listener) Close() error {
	select {
	case l.httpServeErr <- fmt.Errorf("accept on closed listener"):
	}
	l.closed = true
	return l.ln.Close()
}

func (l *Listener) Addr() net.Addr {
	return l.ln.Addr()
}

func (l *Listener) Accept() (net.Conn, error) {
	for {
		select {
		case err := <-l.httpServeErr:
			return nil, err
		case conn := <-l.pendingConns:
			return conn, nil
		}
	}
}

func Listen(network string, address string) (net.Listener, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	l := &Listener{
		ln:            ln,
		httpServeErr:  make(chan error, 1),
		pendingConns:  make(chan *ServerConn, 1024),
		conns:         map[uint32]*ServerConn{},
		InactivePurge: 60 * time.Second,
	}

	l.blk, _ = aes.NewCipher([]byte(network + "0123456789abcdef")[:16])

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", l.handler)
		l.httpServeErr <- http.Serve(ln, mux)
	}()

	if Verbose {
		go func() {
			for range time.Tick(time.Second) {
				vprint("listener active connections: ", len(l.conns), ", global counter: ", globalConnCounter)
			}
		}()
	}

	return l, nil
}

func NewServerConn(idx uint32, ln *Listener) *ServerConn {
	c := &ServerConn{idx: idx}
	c.rev = ln
	c.blk = ln.blk
	c.read = newReadConn(c.idx, c.blk, 's')
	return c
}

func (l *Listener) handler(w http.ResponseWriter, r *http.Request) {
	_connIdx, _ := strconv.ParseUint(r.FormValue("s"), 10, 64)
	connIdx := uint32(_connIdx)

	var conn *ServerConn
	l.connsmu.Lock()
	if sc, _ := l.conns[connIdx]; sc != nil {
		conn = sc
	} else {
		conn = NewServerConn(connIdx, l)
		l.conns[connIdx] = conn
		l.pendingConns <- conn
	}
	l.connsmu.Unlock()

	if datalen, err := conn.read.feedFrames(r.Body); err != nil {
		debugprint("listener feed frames error: ", err, ", ", conn, " will be deleted")
		conn.Close()
		return
	} else if datalen == 0 {
		// Client sent nothing, we treat the request as a ping
		// However too many pings without any valid data are meaningless
		// So we won't reschedule its deadline: it will die as expected
	} else {
		conn.schedPurge.Reschedule(func() { conn.Close() }, time.Now().Add(l.InactivePurge))
	}

	conn.write.mu.Lock()

	f := Frame{
		Idx:     conn.write.counter + 1,
		ConnIdx: conn.idx,
		Data:    conn.write.buf,
	}

	if _, err := io.Copy(w, f.Marshal(conn.blk)); err != nil {
		vprint("failed to response to client, error: ", err)
		conn.read.feedError(err)
		conn.Close()
	} else {
		conn.write.buf = conn.write.buf[:0]
		conn.write.counter++
	}

	conn.write.mu.Unlock()
}

func (c *ServerConn) SetReadDeadline(t time.Time) error {
	c.read.ready.SetWaitDeadline(t)
	return nil
}

func (c *ServerConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return nil
}

func (c *ServerConn) SetWriteDeadline(t time.Time) error {
	// SeverConn can't support write deadline
	return nil
}

func (c *ServerConn) Write(p []byte) (n int, err error) {
	if c.read.closed {
		return 0, ErrClosedConn
	}

	if c.read.err != nil {
		return 0, c.read.err
	}

	c.write.mu.Lock()
	c.write.buf = append(c.write.buf, p...)
	c.write.mu.Unlock()
	return len(p), nil
}

func (c *ServerConn) Read(p []byte) (n int, err error) {
	return c.read.Read(p)
}

func (c *ServerConn) Close() error {
	c.schedPurge.Cancel()
	c.read.close()
	c.rev.connsmu.Lock()
	delete(c.rev.conns, c.idx)
	c.rev.connsmu.Unlock()
	//vprint(c, " delete", c.rev.conns)
	return nil
}

func (c *ServerConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *ServerConn) LocalAddr() net.Addr {
	return c.rev.Addr()
}

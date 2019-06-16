package toh

import (
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type ServerConn struct {
	idx     uint64
	counter uint64
	failure error

	write struct {
		mu      sync.Mutex
		buf     []byte
		counter uint64
	}

	read *readConn
}

type Listener struct {
	ln           net.Listener
	conns        sync.Map
	httpServeErr chan error
	pendingConns chan *ServerConn
}

func (l *Listener) Close() error {
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
		ln:           ln,
		httpServeErr: make(chan error, 1),
		pendingConns: make(chan *ServerConn, 1024),
	}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", l.handler)
		l.httpServeErr <- http.Serve(ln, mux)
	}()

	return l, nil
}

func NewServerConn(idx uint64) *ServerConn {
	c := &ServerConn{idx: idx}
	c.read = newReadConn(c.idx)
	return c
}

func (l *Listener) handler(w http.ResponseWriter, r *http.Request) {
	connIdx, _ := strconv.ParseUint(r.FormValue("s"), 10, 64)
	var conn *ServerConn
	if sc, _ := l.conns.Load(connIdx); sc != nil {
		conn = sc.(*ServerConn)
	} else {
		conn = NewServerConn(connIdx)
		l.conns.Store(connIdx, conn)
		l.pendingConns <- conn
	}
	conn.read.feedFrames(r.Body)

	conn.write.mu.Lock()
	defer conn.write.mu.Unlock()

	f := Frame{
		Idx:       incrWriteFrameCounter(&conn.write.counter),
		StreamIdx: conn.idx,
		Data:      conn.write.buf,
	}

	if _, err := io.Copy(w, f.Marshal()); err != nil {
		conn.failure = err
	} else {
		conn.write.buf = conn.write.buf[:0]
	}
}

func (c *ServerConn) SetReadDeadline(t time.Time) error {
	c.read.ready.SetWaitDeadline(t)
	return nil
}

func (c *ServerConn) SetDeadline(t time.Time) error {
	c.SetWriteDeadline(t)
	c.SetReadDeadline(t)
	return nil
}

func (c *ServerConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (c *ServerConn) Write(p []byte) (n int, err error) {
	c.write.mu.Lock()
	defer c.write.mu.Unlock()
	c.write.buf = append(c.write.buf, p...)
	return len(p), nil
}

func (c *ServerConn) Read(p []byte) (n int, err error) {
	return c.read.Read(p)
}

func (c *ServerConn) Close() error {
	return nil
}

func (c *ServerConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *ServerConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

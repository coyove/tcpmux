package toh

import (
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
	idx        uint64
	counter    uint64
	schedPurge sched.SchedKey

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
	conns        sync.Map
	connsmu      sync.Mutex
	httpServeErr chan error
	pendingConns chan *ServerConn

	InactiveTimeout int64
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
		ln:              ln,
		httpServeErr:    make(chan error, 1),
		pendingConns:    make(chan *ServerConn, 1024),
		InactiveTimeout: 60,
	}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", l.handler)
		l.httpServeErr <- http.Serve(ln, mux)
	}()

	go func() {
		for range time.Tick(time.Second) {
			count := 0
			l.conns.Range(func(k, v interface{}) bool {
				count++
				return true
			})
			vprint("listener active connections: ", count)
		}
	}()

	return l, nil
}

func NewServerConn(idx uint64) *ServerConn {
	c := &ServerConn{idx: idx}
	c.read = newReadConn(c.idx, 's')
	return c
}

func (l *Listener) handler(w http.ResponseWriter, r *http.Request) {
	connIdx, _ := strconv.ParseUint(r.FormValue("s"), 10, 64)

	var conn *ServerConn
	l.connsmu.Lock()
	if sc, _ := l.conns.Load(connIdx); sc != nil {
		conn = sc.(*ServerConn)
	} else {
		conn = NewServerConn(connIdx)
		l.conns.Store(connIdx, conn)
		l.pendingConns <- conn
	}
	l.connsmu.Unlock()

	if datalen, err := conn.read.feedFrames(r.Body); err != nil {
		debugprint("listener feed frames error: ", err, ", ", conn, " will be deleted")
		conn.Close()
		l.conns.Delete(conn.idx)
		return
	} else if datalen == 0 {
		// Client sent nothing, we treat the request as a ping
		// However too many pings without any valid data are meaningless
		// So we won't reschedule its deadline: it will die as expected
	} else {
		conn.schedPurge.Reschedule(func() {
			l.conns.Delete(conn.idx)
		}, time.Now().Add(time.Duration(l.InactiveTimeout)*time.Second))
	}

	conn.write.mu.Lock()
	defer conn.write.mu.Unlock()

	f := Frame{
		Idx:       conn.write.counter + 1,
		StreamIdx: conn.idx,
		Data:      conn.write.buf,
	}

	if _, err := io.Copy(w, f.Marshal()); err != nil {
		vprint("failed to response to client, error: ", err)
		conn.read.feedError(err)
		conn.Close()
		l.conns.Delete(conn.idx)
	} else {
		conn.write.buf = conn.write.buf[:0]
		conn.write.counter++
	}
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
	defer c.write.mu.Unlock()
	c.write.buf = append(c.write.buf, p...)
	return len(p), nil
}

func (c *ServerConn) Read(p []byte) (n int, err error) {
	return c.read.Read(p)
}

func (c *ServerConn) Close() error {
	c.schedPurge.Cancel()
	c.read.close()
	return nil
}

func (c *ServerConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *ServerConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

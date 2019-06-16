package toh

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type ServerConn struct {
	idx        uint64
	counter    uint64
	failure    error
	lastActive time.Time

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
		for t := range time.Tick(time.Second) {
			count, gced := 0, 0
			l.conns.Range(func(k, v interface{}) bool {
				conn := v.(*ServerConn)
				if l.InactiveTimeout > 0 && int64(t.Sub(conn.lastActive).Seconds()) > l.InactiveTimeout {
					l.conns.Delete(k)
					gced++
				}
				count++
				return true
			})
			vprint("listener: active connections: ", count, ", deleted: ", gced)
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
		// Client sent nothing, we receive the request as a ping
		// However too many pings without any valid data are meaningless
		// So we won;t update conn's lastActive
	} else {
		conn.lastActive = time.Now()
	}

	conn.write.mu.Lock()
	defer conn.write.mu.Unlock()

	f := Frame{
		Idx:       conn.write.counter + 1,
		StreamIdx: conn.idx,
		Data:      conn.write.buf,
	}

	if _, err := io.Copy(w, f.Marshal()); err != nil {
		conn.failure = err
		fmt.Println(err)
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
	c.read.close()
	return nil
}

func (c *ServerConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *ServerConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

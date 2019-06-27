package toh

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coyove/common/sched"
)

const (
	PING_OK uint16 = iota + 1
	PING_CLOSED
	PING_OK_VOID
)

type ServerConn struct {
	idx        uint64
	rev        *Listener
	schedPurge sched.SchedKey

	write struct {
		sync.Mutex
		buf     []byte
		counter uint32
	}

	read *readConn
}

type Listener struct {
	ln           net.Listener
	closed       bool
	conns        map[uint64]*ServerConn
	connsmu      sync.Mutex
	httpServeErr chan error
	pendingConns chan *ServerConn
	blk          cipher.Block
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
		ln:           ln,
		httpServeErr: make(chan error, 1),
		pendingConns: make(chan *ServerConn, 1024),
		conns:        map[uint64]*ServerConn{},
	}

	l.blk, _ = aes.NewCipher([]byte(network + "0123456789abcdef")[:16])

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", l.handler)
		l.httpServeErr <- http.Serve(ln, mux)
	}()

	if Verbose {
		go func() {
			for range time.Tick(time.Second * 5) {
				ln := 0
				l.connsmu.Lock()
				for _, conn := range l.conns {
					ln += len(conn.write.buf)
					//vprint(conn, len(conn.write.buf))
				}
				l.connsmu.Unlock()
				vprint("listener active connections: ", len(l.conns), ", pending bytes: ", ln)
			}
		}()
	}

	return l, nil
}

func newServerConn(idx uint64, ln *Listener) *ServerConn {
	c := &ServerConn{idx: idx}
	c.rev = ln
	c.read = newReadConn(c.idx, ln.blk, 's')
	return c
}

func (l *Listener) randomReply(w http.ResponseWriter) {
	p := [256]byte{}
	for {
		if rand.Intn(8) == 0 {
			break
		}
		rand.Read(p[:])
		w.Write(p[:rand.Intn(128)+128])
	}
}

func (l *Listener) handler(w http.ResponseWriter, r *http.Request) {
	hdr, ok := parseframe(r.Body, l.blk)
	if !ok {
		l.randomReply(w)
		return
	}

	switch hdr.options {
	case optSyncConnIdx:
	case optClosed:
		l.connsmu.Lock()
		c := l.conns[hdr.connIdx]
		l.connsmu.Unlock()
		if c != nil {
			vprint(c, " is closing because the other side has closed")
			c.Close()
		}
	case optPing:
		l.connsmu.Lock()
		p := bytes.Buffer{}
		for i := 0; i < len(hdr.data); i += 8 {
			connIdx := binary.BigEndian.Uint64(hdr.data[i : i+8])

			if c := l.conns[connIdx]; c != nil && c.read.err == nil && !c.read.closed {
				if len(c.write.buf) > 0 {
					binary.Write(&p, binary.BigEndian, PING_OK)
				} else {
					binary.Write(&p, binary.BigEndian, PING_OK_VOID)
				}
				c.reschedDeath()
			} else {
				binary.Write(&p, binary.BigEndian, PING_CLOSED)
			}

			binary.Write(&p, binary.BigEndian, connIdx)
		}
		l.connsmu.Unlock()

		f := frame{options: optPing, data: p.Bytes()}
		io.Copy(w, f.marshal(l.blk))
		return
	default:
		l.randomReply(w)
		return
	}
	connIdx := hdr.connIdx

	var conn *ServerConn
	l.connsmu.Lock()
	if sc, _ := l.conns[connIdx]; sc != nil {
		conn = sc
		l.connsmu.Unlock()
	} else {
		// New incoming connection?
		f, ok := parseframe(r.Body, l.blk)
		if !ok || f.options&optHello == 0 || f.connIdx != connIdx {
			l.randomReply(w)
			l.connsmu.Unlock()
			return
		}

		conn = newServerConn(connIdx, l)
		l.conns[connIdx] = conn
		l.connsmu.Unlock()

		l.pendingConns <- conn
		vprint("server: new conn: ", conn)
		conn.reschedDeath()
		return
	}

	if datalen, err := conn.read.feedframes(r.Body); err != nil {
		debugprint("listener feed frames, error: ", err, ", ", conn, " will be deleted")
		conn.Close()
		return
	} else if datalen == 0 && len(conn.write.buf) == 0 {
		// Client sent nothing, we treat the request as a ping
		// However too many pings without:
		//   1) sending any valid data to us
		//   2) we sending any valid data to them
		// are meaningless
		// So we won't reschedule its deadline: it will die as expected
	} else {
		conn.reschedDeath()
	}

	conn.writeTo(w)
}

func (conn *ServerConn) reschedDeath() {
	conn.schedPurge.RescheduleSync(func() { conn.Close() }, time.Now().Add(InactivePurge))
}

func (conn *ServerConn) writeTo(w io.Writer) {

	for i := 0; ; i++ {
		conn.write.Lock()
		if len(conn.write.buf) == 0 {
			conn.write.Unlock()
			return
		}

		f := &frame{
			idx:     conn.write.counter + 1,
			connIdx: conn.idx,
			data:    make([]byte, len(conn.write.buf)),
		}

		copy(f.data, conn.write.buf)
		conn.write.buf = conn.write.buf[:0]
		conn.write.counter++
		conn.write.Unlock()

		deadline := time.Now().Add(InactivePurge - time.Second)
	AGAIN:
		if _, err := io.Copy(w, f.marshal(conn.read.blk)); err != nil {
			if time.Now().Before(deadline) {
				goto AGAIN
			}
			vprint("failed to response to client, error: ", err)
			conn.read.feedError(err)
			conn.Close()
			return
		}
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
	return nil
}

func (c *ServerConn) Write(p []byte) (n int, err error) {
REWRITE:
	if c.read.closed {
		return 0, ErrClosedConn
	}

	if c.read.err != nil {
		return 0, c.read.err
	}

	if len(c.write.buf) > MaxWriteBufferSize {
		vprint("write buffer is full")
		time.Sleep(time.Second)
		goto REWRITE
	}

	c.write.Lock()
	c.write.buf = append(c.write.buf, p...)
	c.write.Unlock()
	return len(p), nil
}

func (c *ServerConn) Read(p []byte) (n int, err error) {
	return c.read.Read(p)
}

func (c *ServerConn) Close() error {
	vprint("server: close conn: ", c)
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

func (c *ServerConn) String() string {
	return fmt.Sprintf("<ServerConn:%x,r:%d,w:%d>", c.idx, c.read.counter, c.write.counter)
}

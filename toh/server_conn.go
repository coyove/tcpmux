package toh

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coyove/common/sched"
)

type ServerConn struct {
	idx        uint32
	rev        *Listener
	counter    uint64
	schedPurge sched.SchedKey

	write struct {
		sync.Mutex
		buf     []byte
		counter uint64
		survey  struct {
			maxwritetime float64
			avgwritetime float64
		}
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
		conns:        map[uint32]*ServerConn{},
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
				ln, max, avg := 0, 0.0, 0.0
				l.connsmu.Lock()
				for _, conn := range l.conns {
					ln += len(conn.write.buf)
					if conn.write.survey.maxwritetime > max {
						max = conn.write.survey.maxwritetime
						avg = (avg + conn.write.survey.avgwritetime) / 2
					}
				}
				l.connsmu.Unlock()
				vprint("listener active conns: ", len(l.conns), ", pending: ", ln, "b, max-wr: ", max, "s, avg-wr: ", avg, "s")
			}
		}()
	}

	return l, nil
}

func newServerConn(idx uint32, ln *Listener) *ServerConn {
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
	connIdx, ok := stringToConnIdx(l.blk, r.Header.Get("ETag"))
	if !ok {
		l.randomReply(w)
		return
	}

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
		l.pendingConns <- conn
		vprint("server: new conn: ", conn)
		l.connsmu.Unlock()
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
		conn.schedPurge.Reschedule(func() { conn.Close() }, time.Now().Add(InactivePurge))
	}

	conn.write.Lock()

	f := frame{
		idx:     conn.write.counter + 1,
		connIdx: conn.idx,
		data:    conn.write.buf,
		next: &frame{
			idx:     conn.read.counter,
			options: optSyncIdx,
			data:    []byte{},
		},
	}

	start := time.Now()
	if _, err := io.Copy(w, f.marshal(conn.read.blk)); err != nil {
		vprint("failed to response to client, error: ", err)
		conn.read.feedError(err)
		conn.Close()
	} else {
		conn.write.buf = conn.write.buf[:0]
		conn.write.counter++
		secs := time.Since(start).Seconds()
		if secs > conn.write.survey.maxwritetime {
			conn.write.survey.maxwritetime = secs
		}
		conn.write.survey.avgwritetime = (conn.write.survey.avgwritetime + secs) / 2
	}

	conn.write.Unlock()
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

	if len(c.write.buf) > MaxWriteBufferSize {
		return 0, ErrBigWriteBuf
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
	return fmt.Sprintf("<ServerConn_%d_read_%v_write_%d>", c.idx, c.read, c.write.counter)
}

package tcpmux

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coyove/goflyway/pkg/rand"
)

var testDialHook func(network, addr string, timeout time.Duration) (net.Conn, error)

type DialPool struct {
	sync.Mutex

	maxConns int

	address string
	conns   Map32

	connsCtr  uint32
	streamCtr uint32

	r *rand.ConcurrentRand

	ErrorCallback func(error) bool
	OnRealDial    func(conn net.Conn)
}

// NewDialer creates a new DialPool, set poolSize to 0 to disable pooling
func NewDialer(addr string, poolSize int) *DialPool {
	dp := &DialPool{
		r:        rand.New(),
		address:  addr,
		maxConns: poolSize,
		conns:    Map32{}.New(),
	}

	return dp
}

// GetConns returns the low-level TCP connections
func (d *DialPool) GetConns() *Map32 {
	return &d.conns
}

// Dial connects to the address stored in the DialPool.
func (d *DialPool) Dial() (net.Conn, error) {
	return d.DialTimeout(0)
}

// DialTimeout acts like Dial but takes a timeout.
func (d *DialPool) DialTimeout(timeout time.Duration) (net.Conn, error) {
	if d.maxConns == 0 {
		return net.DialTimeout("tcp", d.address, timeout)
	}

	newStreamAndSayHello := func(c *connState) (*Stream, error) {
		counter := atomic.AddUint32(&d.streamCtr, 1)
		if counter == 0 {
			counter = atomic.AddUint32(&d.streamCtr, 1)
			// counter may not be 1, but it's ok
		}

		s := newStream(counter, c)
		s.tag = 'c'
		c.streams.Store(s.streamIdx, s)

		_, err := c.conn.Write(makeFrame(s.streamIdx, cmdHello, nil))

		if err != nil {
			c.broadcast(err)
			return nil, err
		}

		// After sending the hello, we wait for the ack, or timed out
		if timeout != 0 {
			select {
			case resp := <-s.writeStateResp:
				if resp != cmdAck {
					return nil, ErrStreamLost
				}
			case <-time.After(timeout):
				return nil, &timeoutError{}
			}
		} else {
			select {
			case resp := <-s.writeStateResp:
				if resp != cmdAck {
					return nil, ErrStreamLost
				}
			}
		}

		return s, nil
	}

	d.conns.Lock()
	if len(d.conns.m) < d.maxConns {
		c := &connState{
			idx:           atomic.AddUint32(&d.connsCtr, 1),
			exitRead:      make(chan bool),
			streams:       Map32{}.New(),
			master:        d.conns,
			timeout:       streamTimeout,
			ErrorCallback: d.ErrorCallback,
		}

		d.conns.m[c.idx] = unsafe.Pointer(c)
		d.conns.Unlock()

		var conn net.Conn
		var err error
		if testDialHook == nil {
			conn, err = net.DialTimeout("tcp", d.address, timeout)
		} else {
			conn, err = testDialHook("tcp", d.address, timeout)
		}
		if err != nil {
			d.conns.Delete(c.idx)
			return nil, err
		}

		if t, _ := conn.(*net.TCPConn); t != nil {
			t.SetNoDelay(true)
		}

		if d.OnRealDial != nil {
			d.OnRealDial(conn)
		}

		c.conn = conn
		go c.start()

		return newStreamAndSayHello(c)
	}
	d.conns.Unlock()

	conn := (*connState)(nil)
	for try := 0; conn == nil || conn.conn == nil; try++ {
		i, ln := 0, d.conns.Len()

		d.conns.IterateConst(func(id uint32, p unsafe.Pointer) bool {
			conn = (*connState)(p)
			if ln-i > 0 && d.r.Intn(ln-i) == 0 && conn.conn != nil {
				// break
				return false
			}
			i++
			return true
		})

		if try > 1e6 && d.ErrorCallback != nil {
			d.ErrorCallback(ErrTooManyTries)
		}
	}

	return newStreamAndSayHello(conn)
}

func (d *DialPool) Count() (conns int, streams int) {
	conns = d.conns.Len()

	d.conns.IterateConst(func(id uint32, p unsafe.Pointer) bool {
		streams += (*connState)(p).streams.Len()
		return true
	})

	return
}

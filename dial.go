package tcpmux

import (
	"errors"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type DialPool struct {
	sync.Mutex

	maxConns int

	address string
	conns   Map32

	connsCtr  uint32
	streamCtr uint32

	ErrorCallback func(error) bool
}

// NewDialer creates a new DialPool, set poolSize to 0 to disable pooling
func NewDialer(addr string, poolSize int) *DialPool {
	dp := &DialPool{
		address:  addr,
		maxConns: poolSize,
		conns:    Map32{}.New(),
	}

	return dp
}

func (d *DialPool) GetConns() *Map32 {
	return &d.conns
}

func (d *DialPool) Dial() (net.Conn, error) {
	return d.DialTimeout(0)
}

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

		// after sending the hello, we wait for the ack
		select {
		case resp := <-s.writeStateResp:
			if resp != cmdAck {
				return nil, ErrStreamLost
			}
		}

		return s, nil
	}

	// not thread-safe, maybe we will have connections more than maxConns
	if d.conns.Len() < d.maxConns {
		c := &connState{
			exitRead:      make(chan bool),
			streams:       Map32{}.New(),
			master:        d.conns,
			ErrorCallback: d.ErrorCallback,
		}
		c.address, _ = net.ResolveTCPAddr("tcp", d.address)

		ctr := atomic.AddUint32(&d.connsCtr, 1)
		c.idx = ctr
		d.conns.Store(ctr, c)

		conn, err := net.DialTimeout("tcp", d.address, timeout)
		if err != nil {
			d.conns.Delete(ctr)
			return nil, err
		}

		conn.(*net.TCPConn).SetNoDelay(true)
		c.conn = conn
		go c.start()

		return newStreamAndSayHello(c)
	}

	conn := (*connState)(nil)
	for try := 0; conn == nil || conn.conn == nil; try++ {
		i, ln := 0, d.conns.Len()

		// ln may change
		d.conns.IterateConst(func(id uint32, p unsafe.Pointer) bool {
			conn = (*connState)(p)
			if ln-i > 0 && rand.Intn(ln-i) == 0 && conn.conn != nil {
				// break
				return false
			}
			i++
			return true
		})

		if try > 1e6 && d.ErrorCallback != nil {
			d.ErrorCallback(errors.New("dial: too many tries of finding a valid conn"))
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

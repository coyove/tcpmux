package tcpmux

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coyove/common/rand"
	"github.com/coyove/common/waitobject"
)

var MasterTimeout uint32 = 20

type DialPool struct {
	sync.Mutex
	address   string
	conns     Map32
	connsCtr  uint32
	streamCtr uint32
	maxConns  uint32
	r         *rand.Rand

	OnError  func(error) bool
	OnDialed func(conn net.Conn)
	OnDial   func(address string) (net.Conn, error)
	Key      []byte
}

// NewDialer creates a new DialPool, set poolSize to 0 to disable pooling
func NewDialer(addr string, poolSize int) *DialPool {
	dp := &DialPool{
		address:  addr,
		maxConns: uint32(poolSize),
		conns:    NewMap32(),
		r:        rand.New(),
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
		if d.OnDial == nil {
			return net.DialTimeout("tcp", d.address, timeout)
		}
		return d.OnDial(d.address)
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

		debugprint("dial new stream: ", s)

		_, err := c.conn.Write(c.makeFrame(s.streamIdx, cmdHello, true, nil))
		if err != nil {
			c.broadcastErrAndStop(err)
			return nil, err
		}

		// After sending the hello, we wait for the ack, or timed out
		waitdeadline := waitobject.Eternal
		if timeout != 0 {
			waitdeadline = time.Now().Add(timeout)
		}

		s.read.SetWaitDeadline(waitdeadline)
		v, ontime := s.read.Wait()
		if !ontime {
			return nil, &timeoutError{}
		}
		debugprint(s, " receive first handshak: ", v.(notify).flag == notifyAck)
		if v.(notify).flag != notifyAck {
			return nil, ErrStreamLost
		}
		return s, nil
	}

	d.conns.Lock()
	if len(d.conns.m) < int(d.maxConns) {
		c := &connState{
			idx:        atomic.AddUint32(&d.connsCtr, 1),
			writeQueue: make(chan writePending, 1024),
			master:     d.conns,
			streams:    NewMap32(),
			timeout:    MasterTimeout,
			key:        d.Key,
			tag:        'c',
		}

		if d.Key != nil {
			c.Sum32 = sumHMACsha256
		} else {
			c.Sum32 = sumCRC32
		}

		d.conns.m[c.idx] = unsafe.Pointer(c)
		d.conns.Unlock()

		var conn net.Conn
		var err error
		if d.OnDial == nil {
			conn, err = net.DialTimeout("tcp", d.address, timeout)
		} else {
			conn, err = d.OnDial(d.address)
		}
		if err != nil {
			d.conns.Delete(c.idx)
			return nil, err
		}

		if t, _ := conn.(*net.TCPConn); t != nil {
			t.SetNoDelay(true)
		}

		if d.OnDialed != nil {
			d.OnDialed(conn)
		}

		c.conn = conn
		c.start()

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

		if try > 1e6 && d.OnError != nil {
			d.OnError(ErrTooManyTries)
		}
	}

	return newStreamAndSayHello(conn)
}

func (d *DialPool) Count() []int {
	conns := make([]int, 0, d.conns.Len())

	d.conns.IterateConst(func(id uint32, p unsafe.Pointer) bool {
		conns = append(conns, (*connState)(p).streams.Len())
		return true
	})

	return conns
}

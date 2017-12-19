package main

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coyove/goflyway/pkg/logg"
)

const (
	readRespChanSize     = 256
	acceptStreamChanSize = 256
	bufferSize           = 32*1024 - 1 // 0x7fff
	streamTimeout        = 20
	version              = 0x89
)

const (
	cmdByte = 0xff

	cmdHello = iota
	cmdAck
	cmdErr
	cmdClose
	cmdPing
)

var (
	errConnClosed = errors.New("use of closed network connection")
	errStreamLost = errors.New("dial: no answer from the remote")
)

type Map32 struct {
	*sync.RWMutex
	m map[uint32]unsafe.Pointer
}

func (Map32) New() Map32 {
	return Map32{RWMutex: new(sync.RWMutex), m: make(map[uint32]unsafe.Pointer)}
}

// Store accepts v only if it is a pointer
func (sm *Map32) Store(id uint32, v interface{}) {
	sm.Lock()
	sm.m[id] = (*[2]unsafe.Pointer)(unsafe.Pointer(&v))[1]
	sm.Unlock()
}

func (sm *Map32) Delete(ids ...uint32) {
	sm.Lock()
	for _, id := range ids {
		delete(sm.m, id)
	}
	sm.Unlock()
}

func (sm *Map32) Load(id uint32) (unsafe.Pointer, bool) {
	sm.RLock()
	s, ok := sm.m[id]
	sm.RUnlock()
	return s, ok
}

func (sm *Map32) First() (s unsafe.Pointer) {
	sm.RLock()
	for _, s = range sm.m {
		break
	}
	sm.RUnlock()
	return
}

func (sm *Map32) Len() int {
	sm.RLock()
	ln := len(sm.m)
	sm.RUnlock()
	return ln
}

// Iterate deletes the entry if its callback returns false
func (sm *Map32) Iterate(callback func(id uint32, s unsafe.Pointer) bool) {
	sm.RLock()
	ids := []uint32{}
	for k, v := range sm.m {
		if !callback(k, v) {
			ids = append(ids, k)
		}
	}
	sm.RUnlock()

	if len(ids) > 0 {
		sm.Delete(ids...)
	}
}

// IterateConst breaks when callback returns false
func (sm *Map32) IterateConst(callback func(id uint32, s unsafe.Pointer) bool) {
	sm.RLock()
	for k, v := range sm.m {
		if !callback(k, v) {
			break
		}
	}
	sm.RUnlock()
}

type readState struct {
	buf []byte
	n   int
	err error
	idx uint32
	cmd byte
}

type Stream struct {
	master *connState

	streamIdx uint32

	readResp        chan *readState
	readOverflowBuf []byte

	writeStateResp chan byte // state returned from remote when writing

	shouldClose bool
	closed      atomic.Value
	readExit    chan bool // inform Read() to exit
	writeExit   chan bool // inform Write() to exit

	tag byte // for debug purpose

	lastActive int64
	timeout    int64
}

func newStream(id uint32, c *connState) *Stream {
	s := &Stream{
		streamIdx:      id,
		master:         c,
		readExit:       make(chan bool, 1),
		writeExit:      make(chan bool, 1),
		readResp:       make(chan *readState, readRespChanSize),
		writeStateResp: make(chan byte, 1),
		timeout:        streamTimeout,
		lastActive:     time.Now().UnixNano(),
	}

	s.closed.Store(false)
	return s
}

func (c *Stream) Read(buf []byte) (n int, err error) {
	if c == nil || c.closed.Load().(bool) {
		return 0, errConnClosed
	}

	if c.readOverflowBuf != nil {
		copy(buf, c.readOverflowBuf)

		if len(c.readOverflowBuf) > len(buf) {
			c.readOverflowBuf = c.readOverflowBuf[len(buf):]
			return len(buf), nil
		}

		n = len(c.readOverflowBuf)
		c.readOverflowBuf = nil
		return
	}

	atomic.StoreInt64(&c.lastActive, time.Now().UnixNano())

	// MORE:
	select {
	// case cmd := <-c.readStateResp:
	// 	if cmd == cmdClose {
	// 		if len(c.readResp) == 0 {
	// 			return 0, errConnClosed
	// 		}
	// 		c.shouldClose = true
	// 	}

	// 	goto MORE
	case x := <-c.readResp:
		// if x.cmd == cmdClose {
		// 	return 0, errConnClosed
		// }

		n, err = x.n, x.err

		if x.buf != nil {
			xbuf := x.buf[:x.n]
			if len(xbuf) > len(buf) {
				c.readOverflowBuf = xbuf[len(buf):]
				n = len(buf)
			}

			copy(buf, xbuf)
		}
	case <-c.readExit:
		return 0, errConnClosed
	}

	atomic.StoreInt64(&c.lastActive, time.Now().UnixNano())
	return
}

// Write is NOT a thread-safe function, it is intended to be used only in one goroutine
func (c *Stream) Write(buf []byte) (n int, err error) {
	if c == nil || c.closed.Load().(bool) {
		return 0, errConnClosed
	}

	header := make([]byte, 7+len(buf))
	binary.BigEndian.PutUint32(header[1:], uint32(c.streamIdx))
	binary.BigEndian.PutUint16(header[5:], uint16(len(buf)))
	header[0] = version

	copy(header[7:], buf)

	writeChan := make(chan bool)
	go func() {
		n, err = c.master.conn.Write(header)
		writeChan <- true
	}()

	select {
	case <-writeChan:
		if err != nil {
			return
		}
	case <-c.writeExit:
		return 0, errConnClosed
	}

	atomic.StoreInt64(&c.lastActive, time.Now().UnixNano())
	select {
	case cmd := <-c.writeStateResp:
		switch cmd {
		case cmdErr:
			return 0, errors.New("write: remote returns an error")
		case cmdClose:
			return 0, errConnClosed
		}
	case <-c.writeExit:
		return 0, errConnClosed
	default:
	}

	atomic.StoreInt64(&c.lastActive, time.Now().UnixNano())
	n -= 7
	return
}

func (c *Stream) closeNoInfo() {
	select {
	case c.readExit <- true:
	default:
	}

	select {
	case c.writeExit <- true:
	default:
	}

	c.closed.Store(true)
}

// Close closes the stream and remove it from its master
func (c *Stream) Close() error {
	c.closeNoInfo()

	buf := []byte{version, 0, 0, 0, 0, cmdByte, cmdClose}
	binary.BigEndian.PutUint32(buf[1:], c.streamIdx)
	if _, err := c.master.conn.Write(buf); err != nil {
		c.master.broadcast(err)
	}

	c.master.streams.Delete(c.streamIdx)
	return nil
}

// SetTimeout sets the timeout for this stream, error range: 1 sec
func (c *Stream) SetTimeout(sec int64) {
	atomic.StoreInt64(&c.timeout, sec)
}

// LocalAddr is a compatible method for net.Conn
func (c *Stream) LocalAddr() net.Addr { return &net.TCPAddr{} }

// RemoteAddr is a compatible method for net.Conn
func (c *Stream) RemoteAddr() net.Addr { return c.master.address }

// SetReadDeadline is a compatible method for net.Conn
func (c *Stream) SetReadDeadline(t time.Time) error { return c.SetDeadline(t) }

// SetWriteDeadline is a compatible method for net.Conn
func (c *Stream) SetWriteDeadline(t time.Time) error { return c.SetDeadline(t) }

// SetDeadline is a compatible method for net.Conn
func (c *Stream) SetDeadline(t time.Time) error {
	sec := (t.UnixNano() - time.Now().UnixNano() + 1e6) / 1e9
	if sec <= 0 {
		c.SetTimeout(0)
		return nil
	}
	c.SetTimeout(sec)
	return nil
}

type connState struct {
	address *net.TCPAddr

	master  Map32
	streams Map32

	idx uint32

	conn     net.Conn
	exitRead chan bool

	newStreamCallback func(state *readState)

	stopped bool
	sync.Mutex
}

// When something serious happened, we broadcast it to every stream and close the master conn
// TCP connections may have temporary errors, but here we treat them as the same as other failures
func (cs *connState) broadcast(err error) {
	logg.E("master conn err: ", err)

	cs.streams.Iterate(func(idx uint32, s unsafe.Pointer) bool {
		(*Stream)(s).readResp <- &readState{err: err}
		return true
	})

	cs.stop()
}

func (cs *connState) start() {
	readChan, daemonChan := make(chan bool), make(chan bool)

	go func() {
		for {
			time.Sleep(2 * time.Second)

			select {
			case <-daemonChan:
				return
			default:
				now := time.Now().UnixNano()
				cs.streams.Iterate(func(idx uint32, p unsafe.Pointer) bool {
					s := (*Stream)(p)
					if s.closed.Load().(bool) {
						// return false to delete
						return false
					}

					if to := atomic.LoadInt64(&s.timeout); to > 0 && (now-s.lastActive)/1e9 <= to {
						return true
					}

					s.closeNoInfo()
					return false
				})
				//logg.D(">>>>>>", cs.streams.Len())
				if _, err := cs.conn.Write([]byte{version, 0, 0, 0, 0, cmdByte, cmdPing}); err != nil {
					cs.broadcast(err)
					return
				}
			}
		}
	}()

	for {
		go func() {

			buf := make([]byte, 7)
			_, err := io.ReadAtLeast(cs.conn, buf, 7)
			if err != nil {
				cs.broadcast(err)
				return
			}

			if buf[0] != version {
				cs.broadcast(errors.New("fatal: invalid header received"))
				return
			}

			streamIdx := binary.BigEndian.Uint32(buf[1:])
			streamLen := int(binary.BigEndian.Uint16(buf[5:]))
			if buf[5] == cmdByte {
				switch buf[6] {
				case cmdHello:
					cs.newStreamCallback(&readState{idx: streamIdx})

					buf[5], buf[6] = cmdByte, cmdAck
					// we acknowledge the hello
					if _, err = cs.conn.Write(buf); err != nil {
						cs.broadcast(err)
						return
					}

					fallthrough
				case cmdPing:
					readChan <- true
					return
				default:
					if p, ok := cs.streams.Load(streamIdx); ok {
						s := (*Stream)(p)
						select {
						case s.writeStateResp <- buf[6]:
						default:
						}

						select {
						case s.readResp <- &readState{cmd: buf[6]}:
						default:
						}
					}
					// if it is an invalid index, we do nothing to prevent infinite loop, next time the sender writes,
					// he won't receive any confirmation, so he knows the receiver is dead
				}

				readChan <- true
				return
			}

			payload := make([]byte, streamLen)
			_, err = io.ReadAtLeast(cs.conn, payload, streamLen)
			rs := &readState{
				n:   streamLen,
				err: err,
				buf: payload,
				idx: streamIdx,
			}

			if s, ok := cs.streams.Load(streamIdx); ok {
				(*Stream)(s).readResp <- rs
			} else {
				buf[5], buf[6] = cmdByte, cmdClose
				if _, err = cs.conn.Write(buf); err != nil {
					cs.broadcast(err)
					return
				}
			}
			readChan <- true
		}()

		select {
		case <-cs.exitRead:
			daemonChan <- true
			return
		default:
		}

		select {
		case <-readChan:
		case <-cs.exitRead:
			daemonChan <- true
			return
		}
	}
}

func (cs *connState) stop() {
	cs.Lock()
	if cs.stopped {
		cs.Unlock()
		return
	}

	cs.exitRead <- true
	cs.streams.Iterate(func(idx uint32, p unsafe.Pointer) bool {
		s := (*Stream)(p)
		s.closeNoInfo()
		return true
	})

	cs.conn.Close()
	cs.master.Delete(cs.idx)

	cs.stopped = true
	cs.Unlock()
}

type DialPool struct {
	sync.Mutex

	maxConns int

	address string
	conns   Map32

	connsCtr  uint32
	streamCtr uint32
}

func NewDialer(addr string, poolSize int) *DialPool {
	dp := &DialPool{
		address:  addr,
		maxConns: poolSize,
		conns:    Map32{}.New(),
	}

	return dp
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

		buf := []byte{version, 0, 0, 0, 0, cmdByte, cmdHello}
		binary.BigEndian.PutUint32(buf[1:], s.streamIdx)
		_, err := c.conn.Write(buf)

		if err != nil {
			c.broadcast(err)
			return nil, err
		}

		// after sending the hello, we wait for the ack
		select {
		case resp := <-s.writeStateResp:
			if resp != cmdAck {
				return nil, errStreamLost
			}
		}

		return s, nil
	}

	// not thread-safe, maybe we will have connections more than maxConns
	if d.conns.Len() < d.maxConns {
		c := &connState{exitRead: make(chan bool), streams: Map32{}.New(), master: d.conns}
		c.address, _ = net.ResolveTCPAddr("tcp", d.address)

		ctr := atomic.AddUint32(&d.connsCtr, 1)
		c.idx = ctr
		d.conns.Store(ctr, c)

		conn, err := net.DialTimeout("tcp", d.address, timeout)
		if err != nil {
			d.conns.Delete(ctr)
			return nil, err
		}

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

		if try > 1e6 {
			logg.W("too many tries of finding a valid conn")
		}
	}

	return newStreamAndSayHello(conn)
}

type ListenPool struct {
	ln net.Listener

	conns   Map32
	streams Map32

	connsCtr  uint32
	streamCtr uint32

	newStreamWaiting chan uint32

	exit  chan bool
	exitA chan bool
}

func Listen(addr string, pooling bool) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	if !pooling {
		return ln, err
	}

	lp := &ListenPool{
		ln:      ln,
		exit:    make(chan bool, 1),
		exitA:   make(chan bool, 1),
		conns:   Map32{}.New(),
		streams: Map32{}.New(),

		newStreamWaiting: make(chan uint32, acceptStreamChanSize),
	}

	go lp.accept()
	return lp, nil
}

func (l *ListenPool) accept() {
	for {
		select {
		case <-l.exit:
			return
		default:
			conn, err := l.ln.Accept()
			if err != nil {
				logg.D("accept: ", err)
				continue
			}

			l.connsCtr++
			var c *connState
			c = &connState{
				idx:      l.connsCtr,
				conn:     conn,
				master:   l.conns,
				exitRead: make(chan bool),
				streams:  Map32{}.New(),
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
		s, _ := l.streams.Load(idx)
		return (*Stream)(s), nil
	case <-l.exitA:
		return nil, errors.New("listener has ended")
	}
}

func (l *ListenPool) Close() error {
	l.exit <- true
	l.exitA <- true
	return nil
}

func (l *ListenPool) Addr() net.Addr {
	return l.ln.Addr()
}

func main() {
	// f, _ := os.Open("z.txt")
	// buf := make([]byte, 102400)
	// f.Read(buf)
	// fmt.Println(string(buf))
	// if true {
	// 	return
	// }

	logg.SetLevel("dbg")
	logg.Start()

	go func() {
		ln, _ := Listen(":13739", true)

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			logg.D("hibegin")
			h := w.(http.Hijacker)
			conn, _, _ := h.Hijack()
			logg.D("hiend")

			conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n" + r.RequestURI[1:]))
			conn.Close()
			// w.Write([]byte(r.RequestURI[1:]))
		})
		http.Serve(ln, nil)

		for {

			conn, _ := ln.Accept()
			go func() {
				go func() {
					time.Sleep(5 * time.Second)
					logg.D("close", conn.Close())
				}()

				{
					// buf := make([]byte, 1024)

					// n, err := conn.Read(buf)
					// logg.D(conn.stream, err, buf[:n])
					// if err != nil {
					// 	return
					// }

					logg.D(conn.Write([]byte("hello world")))
				}
			}()
		}
	}()

	logg.D("z")
	p := NewDialer("127.0.0.1:13739", 1)

	client := http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				return p.Dial()
			},
		},
	}

	random := func() string {
		buf := make([]byte, 16)
		for i := 0; i < 16; i++ {
			buf[i] = byte(rand.Intn(26)) + 'a'
		}
		return string(buf)
	}

	test := func(wg *sync.WaitGroup) {
		str := random()
		resp, err := client.Get("http://127.0.0.1:13739/" + str)

		if err != nil {
			panic(err)
		}

		buf, _ := ioutil.ReadAll(resp.Body)
		if string(buf) != str {
			panic(string(buf))
		}

		resp.Body.Close()
		wg.Done()
	}

	go func() {
		for {
			time.Sleep(30 * time.Second)
			//p.conns[1].conn.Close()
			f, _ := os.Create("heap.txt")
			pprof.Lookup("goroutine").WriteTo(f, 1)
		}
		//time.Sleep(time.Second * 5)

	}()

	for {
		wg := &sync.WaitGroup{}

		//start := time.Now()
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go test(wg)
		}
		wg.Wait()
		//logg.D("take: ", time.Now().Sub(start).Seconds())

		sctr := 0
		p.conns.IterateConst(func(id uint32, p unsafe.Pointer) bool {
			sctr += (*connState)(p).streams.Len()
			return true
		})

		logg.D(p.conns.Len(), " ", sctr)
	}

	select {}
}

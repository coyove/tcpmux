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

	"github.com/coyove/goflyway/pkg/logg"
)

type readState struct {
	buf []byte
	n   int
	err error
	idx uint32
}

const readRespChanSize = 256
const acceptStreamChanSize = 256
const bufferSize = 32*1024 - 1
const version = 137

const (
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

type StreamMap struct {
	sync.RWMutex
	m map[uint32]*Stream
}

func (sm *StreamMap) Store(id uint32, s *Stream) {
	sm.Lock()
	sm.m[id] = s
	sm.Unlock()
}

func (sm *StreamMap) Delete(ids ...uint32) {
	sm.Lock()
	for _, id := range ids {
		delete(sm.m, id)
	}
	sm.Unlock()
}

func (sm *StreamMap) Load(id uint32) (*Stream, bool) {
	sm.RLock()
	s, ok := sm.m[id]
	sm.RUnlock()
	return s, ok
}

func (sm *StreamMap) First() (s *Stream) {
	sm.RLock()
	for _, s = range sm.m {
		break
	}
	sm.RUnlock()
	return
}

func (sm *StreamMap) Len() int {
	sm.RLock()
	ln := len(sm.m)
	sm.RUnlock()
	return ln
}

func (sm *StreamMap) Iterate(f func(id uint32, s *Stream) bool) {
	sm.RLock()
	ids := []uint32{}
	for k, v := range sm.m {
		if !f(k, v) {
			ids = append(ids, k)
		}
	}
	sm.RUnlock()

	if len(ids) > 0 {
		sm.Delete(ids...)
	}
}

type Stream struct {
	master *connState

	stream uint32

	readResp chan *readState

	writeStateResp chan byte
	readStateResp  chan byte

	closed              atomic.Value
	readExit, writeExit chan bool

	readOverflowBuf []byte

	tag byte

	lastActive int64
	timeout    int64
}

func newStream(id uint32, c *connState) *Stream {
	s := &Stream{
		stream:         id,
		master:         c,
		readExit:       make(chan bool, 1),
		writeExit:      make(chan bool, 1),
		readResp:       make(chan *readState, readRespChanSize),
		writeStateResp: make(chan byte, 1),
		readStateResp:  make(chan byte, 1),
		timeout:        100,
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

	select {
	case cmd := <-c.readStateResp:
		if cmd == cmdClose {
			close(c.readStateResp)
			return 0, errConnClosed
		}
	case x := <-c.readResp:
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
	binary.BigEndian.PutUint32(header[1:], uint32(c.stream))
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
		close(c.writeStateResp)
		switch cmd {
		case cmdErr:
			return 0, errors.New("write: remote returns an error")
		case cmdClose:
			return 0, errors.New("write: remote has been closed")
		}
	case <-c.writeExit:
		return 0, errConnClosed
	default:
	}

	n -= 7
	return
}

func (c *Stream) closeNoInfo() {
	c.readExit <- true
	c.writeExit <- true
	c.closed.Store(true)
}

func (c *Stream) Close() error {
	c.closeNoInfo()

	buf := []byte{version, 0, 0, 0, 0, 0xff, cmdClose}
	binary.BigEndian.PutUint32(buf[1:], c.stream)
	if _, err := c.master.conn.Write(buf); err != nil {
		c.master.broadcast(err)
	}

	return nil
}

func (c *Stream) LocalAddr() net.Addr { return &net.TCPAddr{} }

func (c *Stream) RemoteAddr() net.Addr { return &net.TCPAddr{} }

func (c *Stream) SetDeadline(t time.Time) error {
	sec := (t.UnixNano() - time.Now().UnixNano() + 1e6) / 1e9
	if sec <= 0 {
		return nil
	}

	atomic.StoreInt64(&c.timeout, sec)
	return nil
}

func (c *Stream) SetReadDeadline(t time.Time) error { return c.SetDeadline(t) }

func (c *Stream) SetWriteDeadline(t time.Time) error { return c.SetDeadline(t) }

func (c *Stream) cleanup() {
}

type connState struct {
	master map[int64]*connState
	idx    int64

	conn      net.Conn
	exitRead  chan bool
	streams   StreamMap
	maxStream uint32

	newStreamCallback func(state *readState)

	stopped bool
	sync.Mutex
}

func (cs *connState) broadcast(err error) {
	logg.E("master conn err: ", err)

	cs.streams.Iterate(func(idx uint32, s *Stream) bool {
		s.readResp <- &readState{err: err}
		return true
	})

	cs.stop(true)
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
				cs.streams.Iterate(func(idx uint32, s *Stream) bool {
					if s.closed.Load().(bool) {
						// return false to delete
						return false
					}

					if (now-s.lastActive)/1e9 <= atomic.LoadInt64(&s.timeout) {
						return true
					}

					//s.Close()
					return false
				})
				//logg.D(">>>>>>", cs.streams.Len())
				if _, err := cs.conn.Write([]byte{version, 0, 0, 0, 0, 0xff, cmdPing}); err != nil {
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
				cs.broadcast(errors.New("invalid header received"))
				return
			}

			streamIdx := binary.BigEndian.Uint32(buf[1:])
			streamLen := int(binary.BigEndian.Uint16(buf[5:]))
			if buf[5] == 0xff {
				switch buf[6] {
				case cmdHello:
					cs.newStreamCallback(&readState{idx: streamIdx})

					buf[5], buf[6] = 0xff, cmdAck
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
					if s, ok := cs.streams.Load(streamIdx); ok {
						select {
						case s.writeStateResp <- buf[6]:
						default:
							logg.D(buf[6])
						}
						select {
						case s.readStateResp <- buf[6]:
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
				s.readResp <- rs
			} else {
				buf[5], buf[6] = 0xff, cmdClose
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

func (cs *connState) stop(gracefully bool) {
	cs.Lock()
	if cs.stopped {
		cs.Unlock()
		return
	}

	cs.exitRead <- true
	cs.streams.Iterate(func(idx uint32, s *Stream) bool {
		logg.D("connState is closing: ", s.stream)
		s.closeNoInfo()
		return true
	})

	cs.conn.Close()
	delete(cs.master, cs.idx) //TODO

	cs.stopped = true
	cs.Unlock()
}

type DialPool struct {
	sync.Mutex

	maxConns int

	address string
	conns   map[int64]*connState

	connsCtr  int64
	streamCtr int64
}

func NewDialPool(addr string, max int) *DialPool {
	dp := &DialPool{
		address:  addr,
		maxConns: max,
		conns:    make(map[int64]*connState),
	}

	// go func() {
	// 	for {
	// 		for idx, cs := range dp.conns {
	// 			logg.D("conn", idx)
	// 			for _, s := range cs.streams {
	// 				logg.D("    stream", s.stream)
	// 			}
	// 		}
	// 		time.Sleep(5 * time.Second)
	// 	}
	// }()

	return dp
}

func (d *DialPool) Dial() (net.Conn, error) {
	d.Lock()

	counter := func() uint32 {
		c := uint32(atomic.AddInt64(&d.streamCtr, 1))
		if c == 0 {
			c = uint32(atomic.AddInt64(&d.streamCtr, 1))
			// c may not be 1, but it's ok
		}
		return c
	}

	newStreamAndSayHello := func(c *connState) (*Stream, error) {
		s := newStream(counter(), c)
		s.tag = 'c'
		c.streams.Store(s.stream, s)

		buf := []byte{version, 0, 0, 0, 0, 0xff, cmdHello}
		binary.BigEndian.PutUint32(buf[1:], s.stream)
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

	if len(d.conns) < d.maxConns {
		c := &connState{exitRead: make(chan bool), streams: StreamMap{m: make(map[uint32]*Stream)}, master: d.conns}
		d.connsCtr++
		ctr := d.connsCtr
		c.idx = ctr
		d.conns[ctr] = c
		d.Unlock()

		conn, err := net.Dial("tcp", d.address)
		if err != nil {
			d.Lock()
			delete(d.conns, ctr)
			d.Unlock()
			return nil, err
		}

		c.conn = conn
		go c.start()

		return newStreamAndSayHello(c)
	}

	defer d.Unlock()

	ln, _, conn := len(d.conns), int64(0), (*connState)(nil)
	for try := 0; conn == nil || conn.conn == nil; try++ {
		i := 0
		for _, conn = range d.conns {
			if rand.Intn(ln-i) == 0 && conn.conn != nil {
				break
			}
			i++
		}

		// if try > 100 {
		// 	return nil, errors.New("can't fina a valid connection, try again later")
		// }
	}

	return newStreamAndSayHello(conn)
}

type ListenPool struct {
	ln net.Listener

	conns   map[int64]*connState
	streams StreamMap

	connsCtr  int64
	streamCtr int64

	newStreamWaiting chan uint32

	exit  chan bool
	exitA chan bool
}

func NewListenPool(addr string) (*ListenPool, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	lp := &ListenPool{
		ln:               ln,
		exit:             make(chan bool, 1),
		exitA:            make(chan bool, 1),
		conns:            make(map[int64]*connState),
		streams:          StreamMap{m: make(map[uint32]*Stream)},
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
				logg.D("accept:", err)
				continue
			}

			l.connsCtr++
			var c *connState
			c = &connState{
				idx:      l.connsCtr,
				conn:     conn,
				master:   l.conns,
				exitRead: make(chan bool),
				streams:  StreamMap{m: make(map[uint32]*Stream)},
				newStreamCallback: func(state *readState) {
					idx := state.idx
					s := newStream(idx, c)
					s.tag = 's'

					c.streams.Store(idx, s)
					l.streams.Store(idx, s)
					l.newStreamWaiting <- idx

					if idx > c.maxStream {
						c.maxStream = idx
					}
				},
			}

			l.conns[l.connsCtr] = c
			go c.start()
		}
	}
}

func (l *ListenPool) Accept() (net.Conn, error) {
	select {
	case idx := <-l.newStreamWaiting:
		s, _ := l.streams.Load(idx)
		return s, nil
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
	logg.SetLevel("dbg")
	logg.Start()

	go func() {
		ln, _ := NewListenPool(":13739")

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(r.RequestURI[1:]))
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
	p := NewDialPool("127.0.0.1:13739", 1)

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
			time.Sleep(time.Minute)
			//p.conns[1].conn.Close()
			f, _ := os.Create("1.txt")
			pprof.Lookup("goroutine").WriteTo(f, 1)
		}
		//time.Sleep(time.Second * 5)

	}()

	for {
		wg := &sync.WaitGroup{}

		start := time.Now()
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go test(wg)
		}
		wg.Wait()
		logg.D("take: ", time.Now().Sub(start).Seconds())

		sctr := 0
		for _, c := range p.conns {
			sctr += c.streams.Len()
		}

		logg.D(len(p.conns), " ", sctr)
	}

	select {}
}

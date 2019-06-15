package tcpmux

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/coyove/common/rand"
)

const (
	readRespChanSize     = 256
	acceptStreamChanSize = 256
	bufferSize           = 32 * 1024 // 0x8000
	streamTimeout        = 20        // seconds
	pingInterval         = 2         // seconds
)

const (
	cmdHello = iota + 1
	cmdAck
	cmdRemoteClosed
	cmdPayload
)

type notifyFlag byte

func (nf notifyFlag) String() string {
	p := bytes.Buffer{}
	for i := 0; i < 8; i++ {
		n := notifyFlag(1 << uint(i))
		if nf&n > 0 {
			switch n {
			case notifyError:
				p.WriteString("Error")
			case notifyClose:
				p.WriteString("Close")
			case notifyAck:
				p.WriteString("Ack")
			case notifyReady:
				p.WriteString("Ready")
			default:
				p.WriteString("Unknown")
			}
		}
	}
	return p.String()
}

const (
	notifyError notifyFlag = 1 << iota
	notifyClose
	notifyReady
	notifyAck
)

var (
	// ErrConnClosed should be identical to the message of poll.ErrNetClosing
	ErrConnClosed = errors.New("use of closed network connection")

	// ErrStreamLost is returned when a Dial failed
	ErrStreamLost = errors.New("dial: no answer from the remote")

	// ErrTooManyTries is returned when a Dial tried too many times
	ErrTooManyTries = errors.New("dial: too many tries of finding a valid conn")

	// ErrInvalidHash is returned when the stream doesn't start with a valid version
	ErrInvalidHash = errors.New("fatal: invalid hash")

	// ErrLargeWrite is returned when payload is too large to write
	ErrLargeWrite = errors.New("can't write large buffer which exceeds 65535 bytes")
)

func (conn *connState) makeFrame(idx uint32, cmd byte, mask bool, payload []byte) []byte {
	p := &bytes.Buffer{}

	if cmd != cmdPayload {
		buf := []byte{0, 0, 0, 0, 0, 0, 0, 0, cmd}
		binary.BigEndian.PutUint32(buf[4:], idx)
		binary.BigEndian.PutUint32(buf[:4], conn.Sum32(buf[4:], conn.key))

		WSWrite(p, buf, mask)
		return p.Bytes()
	}

	header := make([]byte, 9+len(payload))
	binary.BigEndian.PutUint32(header[4:], uint32(idx))
	header[8] = cmdPayload
	copy(header[9:], payload)

	binary.BigEndian.PutUint32(header[:4], conn.Sum32(header[4:], conn.key))

	WSWrite(p, header, mask)
	return p.Bytes()
}

type timeoutError struct{}

func (e *timeoutError) Error() string { return "operation timed out" }

func (e *timeoutError) Timeout() bool { return true }

func (e *timeoutError) Temporary() bool { return false }

// Map32 is a mapping from uint32 to unsafe.Pointer
type Map32 struct {
	*sync.RWMutex
	m map[uint32]unsafe.Pointer
}

// New creates a new Map32
func NewMap32() Map32 {
	return Map32{RWMutex: new(sync.RWMutex), m: make(map[uint32]unsafe.Pointer)}
}

// Clear clears all entries
func (sm *Map32) Clear() {
	sm.Lock()
	sm.m = make(map[uint32]unsafe.Pointer)
	sm.Unlock()
}

// Store accepts v only if it is a pointer
func (sm *Map32) Store(id uint32, v interface{}) {
	sm.Lock()
	sm.m[id] = (*[2]unsafe.Pointer)(unsafe.Pointer(&v))[1]
	sm.Unlock()
}

// Delete deletes multiple entries by ids
func (sm *Map32) Delete(ids ...uint32) {
	sm.Lock()
	for _, id := range ids {
		delete(sm.m, id)
	}
	sm.Unlock()
}

// Load returns the entry by id
func (sm *Map32) Load(id uint32) (unsafe.Pointer, bool) {
	sm.RLock()
	s, ok := sm.m[id]
	sm.RUnlock()
	return s, ok
}

// Fetch returns the entry after removing it from the map
func (sm *Map32) Fetch(id uint32) (unsafe.Pointer, bool) {
	sm.Lock()
	s, ok := sm.m[id]
	delete(sm.m, id)
	sm.Unlock()
	return s, ok
}

// First returns the first entry
func (sm *Map32) First() (s unsafe.Pointer) {
	sm.RLock()
	for _, s = range sm.m {
		break
	}
	sm.RUnlock()
	return
}

// Len returns the total number of entries
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

// WSWrite and WSRead are simple implementations of RFC6455
// we assume that all payloads are 65535 bytes at max
// we don't care control frames and everything is binary
// we don't close it explicitly, it closes when the TCP connection closes
// we don't ping or pong
//
//   0                   1                   2                   3
//   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//   +-+-+-+-+-------+-+-------------+-------------------------------+
//   |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
//   |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
//   |N|V|V|V|       |S|             |   (if payload len==126/127)   |
//   | |1|2|3|       |K|             |                               |
//   +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
//   |     Extended payload length continued, if payload len == 127  |
//   + - - - - - - - - - - - - - - - +-------------------------------+
//   |                               |Masking-key, if MASK set to 1  |
//   +-------------------------------+-------------------------------+
//   | Masking-key (continued)       |          Payload Data         |
//   +-------------------------------- - - - - - - - - - - - - - - - +
//   :                     Payload Data continued ...                :
//   + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
//   |                     Payload Data continued ...                |
//   +---------------------------------------------------------------+
func WSWrite(dst io.Writer, payload []byte, mask bool) (n int, err error) {
	if len(payload) > 65535 {
		return 0, fmt.Errorf("don't support payload larger than 65535 yet")
	}

	buf := make([]byte, 4)
	buf[0] = 2 // binary
	buf[1] = 126
	binary.BigEndian.PutUint16(buf[2:], uint16(len(payload)))

	if mask {
		buf[1] |= 0x80
		buf = append(buf, 0, 0, 0, 0)
		key := uint32(rand.GetCounter())
		binary.BigEndian.PutUint32(buf[4:8], key)

		for i, b := 0, buf[4:8]; i < len(payload); i++ {
			payload[i] ^= b[i%4]
		}
	}

	if n, err = dst.Write(buf); err != nil {
		return
	}

	return dst.Write(payload)
}

func WSRead(src io.Reader) (payload []byte, n int, err error) {
	buf := make([]byte, 4)
	if n, err = io.ReadAtLeast(src, buf[:2], 2); err != nil {
		return
	}

	if buf[0] != 2 {
		err = fmt.Errorf("invalid websocket opcode: %v", buf[0])
		return
	}

	mask := (buf[1] & 0x80) > 0
	ln := int(buf[1] & 0x7f)

	switch ln {
	case 126:
		if n, err = io.ReadAtLeast(src, buf[2:4], 2); err != nil {
			return
		}
		ln = int(binary.BigEndian.Uint16(buf[2:4]))
	case 127:
		err = ErrLargeWrite
		return
	default:
	}

	if mask {
		if n, err = io.ReadAtLeast(src, buf[:4], 4); err != nil {
			return
		}
		// now buf contains mask key
	}

	payload = make([]byte, ln)
	if n, err = io.ReadAtLeast(src, payload, ln); err != nil {
		return
	}

	if mask {
		for i, b := 0, buf[:4]; i < len(payload); i++ {
			payload[i] ^= b[i%4]
		}
	}

	// n == ln, err == nil,
	return
}

func sumCRC32(p, key []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(p)
	return h.Sum32()
}

func sumHMACsha256(p, key []byte) uint32 {
	h := hmac.New(sha256.New, key)
	p = h.Sum(p)
	return binary.BigEndian.Uint32(p[:4])
}

var debug = false

func debugprint(v ...interface{}) {
	if !debug {
		return
	} else {
		//		time.Sleep(time.Millisecond * 100)
		//return
	}

	src, i := bytes.Buffer{}, 1
	for {
		_, fn, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		i++
		src.WriteString(fmt.Sprintf("%s:%d/", filepath.Base(fn), line))
	}
	if src.Len() > 0 {
		src.Truncate(src.Len() - 1)
	}
	fmt.Println(src.String(), "]\n\t", fmt.Sprint(v...))
}

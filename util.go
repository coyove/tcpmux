package tcpmux

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"runtime"
	"sync"
	"unsafe"
)

const (
	readRespChanSize     = 256
	acceptStreamChanSize = 256
	bufferSize           = 32 * 1024 // 0x8000
	streamTimeout        = 20        // seconds
	pingInterval         = 2         // seconds
	cmdByte              = 0xff
)

const (
	cmdHello = iota + 1
	cmdAck
	cmdRemoteClosed
)

const (
	notifyRemoteClosed = 1 << iota
	notifyClose
	notifyCancel
	notifyError
	notifyReady
)

const (
	// OptErrWhenClosed lets Read() and Write() report ErrConnClosed when remote closed
	OptErrWhenClosed = 1 << iota
)

var (
	// ErrConnClosed should be identical to the message of poll.ErrNetClosing
	ErrConnClosed = errors.New("use of closed network connection")

	// ErrStreamLost is returned when a Dial failed
	ErrStreamLost = errors.New("dial: no answer from the remote")

	// ErrTooManyTries is returned when a Dial tried too many times
	ErrTooManyTries = errors.New("dial: too many tries of finding a valid conn")

	// ErrInvalidVerHdr is returned when the stream doesn't start with a valid version
	ErrInvalidHash = errors.New("fatal: invalid hash")

	ErrLargeWrite = fmt.Errorf("can't write large buffer which exceeds %d bytes", bufferSize)
)

var (
	HashSeed []byte
)

func fnv32SH() hash.Hash32 {
	h := fnv.New32()
	h.Write(HashSeed)
	return h
}

func makeFrame(idx uint32, cmd byte, payload []byte) []byte {
	h := fnv32SH()
	sum := func() uint16 {
		s := uint16(h.Sum32())
		s |= 0x8000
		return uint16(s)
	}

	if cmd != 0 {
		buf := []byte{0, 0, 0, 0, 0, 0, cmdByte, cmd}
		binary.BigEndian.PutUint32(buf[2:], idx)
		h.Write(buf[2:])
		binary.BigEndian.PutUint16(buf[:2], sum())
		return buf
	}

	header := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(header[2:], uint32(idx))
	binary.BigEndian.PutUint16(header[6:], uint16(len(payload)))
	copy(header[8:], payload)
	h.Write(header[2:])
	binary.BigEndian.PutUint16(header[:2], sum())
	return header
}

type timeoutError struct{}

func (e *timeoutError) Error() string { return "operation timed out" }

func (e *timeoutError) Timeout() bool { return true }

func (e *timeoutError) Temporary() bool { return false }

func clearCancel(queue chan byte) {
	select {
	case code := <-queue:
		if code == notifyCancel {
			return
		}

		select {
		case queue <- code:
		default:
		}
	default:
	}
}

// Map32 is a mapping from uint32 to unsafe.Pointer
type Map32 struct {
	*sync.RWMutex
	m map[uint32]unsafe.Pointer
}

// New creates a new Map32
func (Map32) New() Map32 {
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

type Survey interface {
	Count() []int
}

func stacktrace() string {
	x := make([]byte, 4096)
	n := runtime.Stack(x, false)
	return string(x[:n])
}

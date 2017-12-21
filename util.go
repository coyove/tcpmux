package tcpmux

import (
	"encoding/binary"
	"errors"
	"sync"
	"unsafe"
)

const (
	readRespChanSize     = 256
	acceptStreamChanSize = 256
	bufferSize           = 32*1024 - 1 // 0x7fff
	streamTimeout        = 20
	version              = 0x89
)

const cmdByte = 0xff

const (
	cmdHello = iota + 1
	cmdAck
	cmdErr
	cmdClose
	cmdPing
)

const (
	notifyExit = iota
	notifyCancel
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
)

func makeFrame(idx uint32, cmd byte, payload []byte) []byte {
	if cmd != 0 {
		buf := []byte{version, 0, 0, 0, 0, cmdByte, cmd}
		binary.BigEndian.PutUint32(buf[1:], idx)
		return buf
	}

	header := make([]byte, 7+len(payload))
	binary.BigEndian.PutUint32(header[1:], uint32(idx))
	binary.BigEndian.PutUint16(header[5:], uint16(len(payload)))
	header[0] = version
	copy(header[7:], payload)
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

func (Map32) New() Map32 {
	return Map32{RWMutex: new(sync.RWMutex), m: make(map[uint32]unsafe.Pointer)}
}

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

// Fetch fetches the pointer and delete it from the map
func (sm *Map32) Fetch(id uint32) (unsafe.Pointer, bool) {
	sm.Lock()
	s, ok := sm.m[id]
	delete(sm.m, id)
	sm.Unlock()
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

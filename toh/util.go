package toh

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	debug   = false
	Verbose = true
)

type timeoutError struct{}

func (e *timeoutError) Error() string {
	return "operation timed out"
}

func (e *timeoutError) Timeout() bool {
	return true
}

func (e *timeoutError) Temporary() bool {
	return false
}

func debugprint(v ...interface{}) {
	if !debug {
		return
	}

	for i := 0; i < len(v); i++ {
		if buf, _ := v[i].([]byte); buf != nil {
			v[i] = string(buf)
		}
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

func vprint(v ...interface{}) {
	if !Verbose {
		return
	}
	_, fn, line, _ := runtime.Caller(1)
	fmt.Println(fmt.Sprintf("%s %s:%d] ", time.Now().Format("Jan _2 15:04:05.000"), filepath.Base(fn), line), fmt.Sprint(v...))
}

var countermark uint32

func newConnectionIdx() uint64 {
	// 25bit timestamp (1 yr) | 16bit counter | 23bit random values
	now := uint32(time.Now().Unix())
	c := atomic.AddUint32(&countermark, 1)
	return uint64(now)<<39 | uint64(c&0xffff)<<23 | uint64(rand.Uint32()&0x7fffff)
}

func frameTmpPath(connIdx uint64, idx uint32) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("%x-%d.toh", connIdx, idx))
}

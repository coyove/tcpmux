package toh

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	strip := func(fn string) string {
		fn = filepath.Base(fn)
		return fn[:len(fn)-3] // ".go"
	}

	_, fn, line, _ := runtime.Caller(1)
	_, fn2, line2, _ := runtime.Caller(2)

	now := time.Now().Format("Jan _2 15:04:05.000")
	str := fmt.Sprint(v...)

	if !strings.HasSuffix(fn2, ".go") {
		fmt.Println(fmt.Sprintf("%s %s:%d] ", now, strip(fn), line), str)
	} else if fn == fn2 {
		fmt.Println(fmt.Sprintf("%s %s:%d\u00bb%d] ", now, strip(fn), line2, line), str)
	} else {
		fmt.Println(fmt.Sprintf("%s %s:%d\u00bb%s:%d] ", now, strip(fn2), line2, strip(fn), line), str)
	}
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

type BufConn struct {
	net.Conn
	*bufio.Reader
}

func NewBufConn(conn net.Conn) *BufConn {
	return &BufConn{Conn: conn, Reader: bufio.NewReader(conn)}
}

func (c *BufConn) Write(p []byte) (int, error) {
	return c.Conn.Write(p)
}

func (c *BufConn) Read(p []byte) (int, error) {
	return c.Reader.Read(p)
}

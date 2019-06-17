package toh

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	debug          = false
	randomFailures = false
	Verbose        = true
)

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

func vprint(v ...interface{}) {
	if !Verbose {
		return
	}
	_, fn, line, _ := runtime.Caller(1)
	fmt.Println(fmt.Sprintf("%s %s:%d] ", time.Now().Format("Jan _2 15:04:05.000"), filepath.Base(fn), line), fmt.Sprint(v...))
}

func incrWriteFrameCounter(counter *uint64) uint64 {
	x := atomic.AddUint64(counter, 1)
	if x == 0 {
		x = atomic.AddUint64(counter, 1)
	}
	return x
}

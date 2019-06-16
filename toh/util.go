package toh

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
)

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

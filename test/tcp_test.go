package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"

	"github.com/coyove/tcpmux"
)

func getListerner() net.Listener {
	ln, err := tcpmux.Listen(":13739", true)
	if err != nil {
		panic(err)
	}

	return ln
}

func TestTCPServer(t *testing.T) {
	ready, exit := make(chan bool), make(chan bool)
	str := randomString()

	go func() {
		ln := getListerner()
		ready <- true

		conn, err := ln.Accept()
		if err != nil {
			panic(err)
			return
		}

		buf := make([]byte, len(str))
		conn.Read(buf)

		if string(buf) != str {
			panic(buf)
		}

		conn.Close()
		ln.Close()
		exit <- true
	}()

	select {
	case <-ready:
	}

	num := 1
	d := tcpmux.NewDialer("127.0.0.1:13739", num)
	conn, _ := d.Dial()
	_, err := conn.Write([]byte(str))
	if err != nil {
		panic(err)
	}

	conn.Close()

	select {
	case <-exit:
	}
}

func TestTCPServerMultiWrite(t *testing.T) {
	stringTransfer(rand.Intn(10)+10, 100000, true)
}

func BenchmarkTCPServerMuxMultiWrite10(b *testing.B) {
	stringTransfer(10, b.N, true)
}

func BenchmarkTCPServerMultiWrite10(b *testing.B) {
	stringTransfer(0, b.N, false)
}

func stringTransfer(num, loop int, pool bool) {
	ready, exit := make(chan bool), make(chan bool)
	strMap := make(map[int]string)
	strLock := sync.Mutex{}

	go func() {
		var ln net.Listener
		if pool {
			ln = getListerner()
		} else {
			ln, _ = tcpmux.Listen(":13739", false)
		}
		ready <- true

		i := 0
		wg := sync.WaitGroup{}
		for {
			conn, err := ln.Accept()
			if err != nil {
				panic(err)
				return
			}

			wg.Add(1)
			go func(conn net.Conn) {
				buf := make([]byte, 40)
				n, err := conn.Read(buf)
				if err != nil {
					panic(err)
				}

				buf = buf[:n]
				idx := int(binary.BigEndian.Uint32(buf))
				ln := int(binary.BigEndian.Uint32(buf[4:]))

				strLock.Lock()
				if string(buf[8:8+ln]) != strMap[idx] {
					panic(fmt.Sprintf("%d: %v", len(strMap), buf))
				}
				delete(strMap, idx)
				strLock.Unlock()

				conn.Close()
				wg.Done()
			}(conn)

			if i++; i == loop {
				wg.Wait()
				break
			}
		}

		ln.Close()
		exit <- true
	}()

	select {
	case <-ready:
	}

	d := tcpmux.NewDialer("127.0.0.1:13739", num)
	var total, count int
	if num == 0 {
		if loop < 1000 {
			count = loop
			total = 1
		} else {
			count = 1000
			total = loop / count
		}
	} else {
		total = 1
		count = loop
	}

	for n := 0; n < total; n++ {
		wg := sync.WaitGroup{}
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) {
				conn, _ := d.Dial()
				str := randomString()
				strLock.Lock()
				strMap[i] = str
				strLock.Unlock()

				buf := append([]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte(str)...)
				binary.BigEndian.PutUint32(buf, uint32(i))
				binary.BigEndian.PutUint32(buf[4:], uint32(len(str)))

				_, err := conn.Write(buf)
				if err != nil {
					panic(err)
				}
				conn.Close()
				wg.Done()
			}(i)
		}
		wg.Wait()
	}

	select {
	case <-exit:
	}
}

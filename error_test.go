package tcpmux

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

func TestTCPServerCloseWhenWrite(t *testing.T) {
	ready, exit := make(chan bool), make(chan bool)

	go func() {
		ln := getListerner()
		ready <- true

		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}

		conn.Close()
		exit <- true
		ln.Close()
		exit <- true
	}()

	select {
	case <-ready:
	}

	d := NewDialer("127.0.0.1:13739", 1)
	conn, err := d.Dial()

	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.Read([]byte{})
	if err != io.EOF {
		t.Fatal(err)
	}
	conn.Close()

	select {
	case <-exit:
	}

	select {
	case <-exit:
	}
}

type testConnReadHook struct {
	net.Conn
	read func(buf []byte) (int, error)
}

func (c *testConnReadHook) Read(buf []byte) (int, error) {
	return c.read(buf)
}

// func TestTCPServerCloseMaster(t *testing.T) {
// 	ready := make(chan bool)
// 	exit := false

// 	var ln net.Listener
// 	go func() {
// 		ln = getListerner()
// 		ready <- true

// 		for {
// 			conn, err := ln.Accept()
// 			if err != nil {
// 				break
// 			}

// 			go func() {
// 				buf := make([]byte, 40)
// 				n, err := conn.Read(buf)
// 				if err != nil {
// 					t.Log("closeMaster:", err)
// 				}

// 				conn.Write(buf[:n])
// 				conn.Close()
// 			}()
// 		}
// 	}()

// 	select {
// 	case <-ready:
// 	}

// 	d := NewDialer("127.0.0.1:13739", 10)

// 	go func() {
// 		time.Sleep(time.Second)
// 		// count, _ := d.Count()
// 		conns := make([]*connState, 0, d.conns.Len())

// 		// can't do it in iteration, deadlock
// 		d.conns.IterateConst(func(id uint32, p unsafe.Pointer) bool {
// 			conns = append(conns, (*connState)(p))
// 			return true
// 		})

// 		for _, conn := range conns {
// 			conn.stop()
// 		}

// 		time.Sleep(time.Second)
// 		log.Println(d.Count())
// 	}()

// 	for !exit {
// 		wg := sync.WaitGroup{}
// 		for i := 0; i < 1000; i++ {
// 			wg.Add(1)
// 			go func(i int) {
// 				defer func() { wg.Done() }()

// 				conn, err := d.Dial()
// 				if err != nil {
// 					t.Log("closeMaster:", err)
// 					exit = true
// 					return
// 				}

// 				time.Sleep(100 * time.Millisecond)
// 				str := randomString()
// 				_, err = conn.Write([]byte(str))
// 				if err != nil {
// 					t.Log("closeMaster:", err)
// 					exit = true
// 					return
// 				}

// 				conn.Close()
// 			}(i)
// 		}
// 		wg.Wait()
// 	}

// 	ln.Close()
// }

func TestHTTPServerConnClosed(t *testing.T) {
	//debug = true
	go func() {
		for {
			time.Sleep(2 * time.Second)
			f, _ := os.Create("heap.txt")
			pprof.Lookup("goroutine").WriteTo(f, 1)
			fmt.Println("profile")
		}
	}()

	var ln net.Listener
	ready := make(chan bool)
	go func() {
		ln = getListerner()
		ready <- true
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			h := w.(http.Hijacker)
			conn, _, _ := h.Hijack()
			conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
			conn.Close()
		})

		http.Serve(ln, mux)
	}()

	select {
	case <-ready:
	}

	num := 10
	p := NewDialer("127.0.0.1:13739", num)

	client := http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				s, err := p.Dial()
				if err == nil {
					s.(*Stream).SetInactiveTimeout(2)
				}
				return s, err
			},
		},
		Timeout: 2 * time.Second,
	}

	exit := false
	test := func(wg *sync.WaitGroup) {
		_, err := client.Get("http://127.0.0.1:13739/")

		if err != nil {
			exit = true
			t.Log(err)
		}

		wg.Done()
	}

	go func() {
		time.Sleep(time.Second)
		(*(*net.Conn)(p.GetConns().First())).Close()
	}()

	for !exit {
		wg := &sync.WaitGroup{}

		for i := 0; i < num*10; i++ {
			wg.Add(1)
			go test(wg)
		}

		wg.Wait()
	}

	ln.Close()

	// This test should be finished in either 1 second or 3 seconds:
	// 1 sec: net.Conn was closed and we immediately know it
	// 3 sec: net.Conn was closed after 1 sec, and http requests timed out in 2 secs
}

func TestSimpleDial(t *testing.T) {
	// non-existed address
	p := NewDialer("1.2.3.4:5678", 10)
	_, err := p.DialTimeout(time.Second)
	if !err.(net.Error).Timeout() {
		t.Fatal("failed")
	}
}

func TestReadDeadline(t *testing.T) {
	ready := make(chan bool, 1)
	go func() {
		ln := getListerner()
		ready <- true
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}

		time.Sleep(2 * time.Second)
		conn.Write([]byte{1})
		conn.Close()
		ln.Close()
	}()

	select {
	case <-ready:
	}

	//debug = true
	p := NewDialer(":13739", 10)
	conn, err := p.Dial()
	if err != nil {
		panic(err)
	}

	buf := []byte{0}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err = conn.Read(buf)
	if !err.(net.Error).Timeout() {
		t.Fatal("failed")
	}

	time.Sleep(2 * time.Second)
	_, err = (conn.Read(buf))
	if !err.(net.Error).Timeout() {
		t.Fatal("failed")
	}

	conn.SetReadDeadline(time.Time{})
	n, err := conn.Read(buf)
	if n != 1 || err != nil {
		t.Fatal("failed")
	}

	_, err = conn.Read(buf)
	if err != io.EOF {
		t.Fatal("failed")
	}

	conn.Close()
}

func TestReadDeadlineNet(t *testing.T) {
	ready := make(chan bool, 1)
	go func() {
		ln, _ := net.Listen("tcp", ":13739")
		ready <- true
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}

		time.Sleep(2 * time.Second)
		conn.Write([]byte{1})
		conn.Close()
		ln.Close()
	}()

	select {
	case <-ready:
	}

	// debug = true
	conn, err := net.Dial("tcp", ":13739")
	if err != nil {
		panic(err)
	}

	buf := []byte{0}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err = conn.Read(buf)
	if !err.(net.Error).Timeout() {
		t.Fatal("failed")
	}

	time.Sleep(2 * time.Second)
	_, err = (conn.Read(buf))
	log.Println(buf)
	if !err.(net.Error).Timeout() {
		t.Fatal("failed")
	}

	conn.SetReadDeadline(time.Time{})
	n, err := conn.Read(buf)
	if n != 1 || err != nil {
		t.Fatal("failed")
	}

	_, err = conn.Read(buf)
	if err != io.EOF {
		t.Fatal("failed")
	}

	conn.Close()
}

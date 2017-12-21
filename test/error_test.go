package main

import (
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coyove/tcpmux"
)

func TestTCPServerCloseWhenWrite(t *testing.T) {
	closeWhenWrite(true)
	closeWhenWrite(false)
}

func closeWhenWrite(flag bool) {
	ready, exit := make(chan bool), make(chan bool)

	go func() {
		ln := getListerner()
		ready <- true

		conn, err := ln.Accept()
		if err != nil {
			panic(err)
			return
		}

		conn.Close()
		ln.Close()
		exit <- true
	}()

	select {
	case <-ready:
	}

	d := tcpmux.NewDialer("127.0.0.1:13739", 1)
	conn, _ := d.Dial()

	if flag {
		conn.(*tcpmux.Stream).SetStreamOpt(tcpmux.OptErrWhenClosed)
	}

	_, err := conn.Read([]byte{})

	if flag {
		if err != tcpmux.ErrConnClosed {
			panic(err)
		}
	} else {
		if err != io.EOF {
			panic(err)
		}
	}
	conn.Close()

	select {
	case <-exit:
	}
}

func TestHTTPServerConnClosed(t *testing.T) {
	go func() {
		ln := getListerner()

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			h := w.(http.Hijacker)
			conn, _, _ := h.Hijack()
			conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
			conn.Close()
		})

		http.Serve(ln, nil)
	}()

	num := 10
	p := tcpmux.NewDialer("127.0.0.1:13739", num)

	client := http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				s, err := p.Dial()
				if err == nil {
					s.(*tcpmux.Stream).SetStreamOpt(tcpmux.OptErrWhenClosed)
					s.(*tcpmux.Stream).SetTimeout(2)
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
			if ne, _ := err.(net.Error); (ne != nil && ne.Timeout()) ||
				strings.Contains(err.Error(), tcpmux.ErrConnClosed.Error()) {
				exit = true
			} else {
				panic(err)
			}
		}

		wg.Done()
	}

	go func() {
		time.Sleep(time.Second)
		// p.GetConns().Iterate(func(id uint32, p unsafe.Pointer) bool {
		// 	(*(*net.Conn)(p)).Close()
		// 	return true
		// })
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

	// This test should be finished in either 1 second or 3 seconds:
	// 1 sec: net.Conn was closed and we immediately know it
	// 3 sec: net.Conn was closed after 1 sec, and http requests timed out in 2 secs
}

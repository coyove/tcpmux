package main

import (
	"net"
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

func TestTCPServerMulti(t *testing.T) {
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

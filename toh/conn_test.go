package toh

import (
	"fmt"
	"testing"
)

func TestClientConn(t *testing.T) {
	debug = true
	go func() {
		ln, _ := Listen("tcp", "127.0.0.1:13739")
		conn, _ := ln.Accept()
		p := [1]byte{}
		conn.Read(p[:])
		fmt.Println(p)
	}()

	conn, _ := Dial("tcp", "127.0.0.1:13739")
	conn.Write([]byte{1})

	select {}
}

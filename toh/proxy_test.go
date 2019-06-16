package toh

import (
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"testing"
)

type client int

type server int

func (s *client) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}

	up, _ := Dial("tcp", ":10001")
	up.Write([]byte("GET " + host + " HTTP/1.1\r\nHost: " + host + "\r\n\r\n"))

	down, _, _ := w.(http.Hijacker).Hijack()

	go func() { io.Copy(up, down) }()
	go func() { io.Copy(down, up) }()
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	log.Println("Server:", host)

	up, _ := net.Dial("tcp", host)
	down, _, _ := w.(http.Hijacker).Hijack()

	if up == nil || down == nil {
		down.Write([]byte("HTTP/1.1 503 Service Unavailable\r\n\r\n"))
		return
	}

	down.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

	go func() { io.Copy(up, down) }()
	go func() { io.Copy(down, up) }()
}

func TestProxy(t *testing.T) {
	go func() {
		log.Println("hello")
		go http.ListenAndServe(":10000", new(client))

		ln, _ := Listen("tcp", ":10001")
		http.Serve(ln, new(server))
	}()

	//	Verbose = false
	select {}
}

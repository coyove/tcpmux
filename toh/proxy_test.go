package toh

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func iocopy(dst io.Writer, src io.Reader) (written int64, err error) {
	size := 32 * 1024
	buf := make([]byte, size)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

func bridge(a, b io.ReadWriteCloser) {
	go func() { iocopy(a, b); a.Close(); b.Close() }()
	go func() { iocopy(b, a); a.Close(); b.Close() }()
}

type client struct {
	upstream string
}

type server int

func (s *client) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}

	up, err := Dial("tcp", s.upstream)
	if err != nil {
		log.Println(err)
		return
	}
	up.Write([]byte(r.Method[:1] + host + "\n"))

	down, _, _ := w.(http.Hijacker).Hijack()
	if r.Method != "CONNECT" {
		header, _ := httputil.DumpRequestOut(r, false)
		x := string(header)
		up.Write([]byte(x))
		io.Copy(up, r.Body)
	}

	bridge(down, up)
}

func foo(conn net.Conn) {
	down := bufio.NewReader(conn)
	buf, err := down.ReadBytes('\n')
	if err != nil || len(buf) < 2 {
		conn.Close()
		return
	}

	host := string(buf)
	connect := host[0] == 'C'
	host = host[1 : len(host)-1]
	// vprint(host, connect)

	up, _ := net.Dial("tcp", host)

	if up == nil || down == nil {
		conn.Write([]byte("HTTP/1.1 503 Service Unavailable\r\n\r\n"))
		return
	}

	if connect {
		conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	}

	dd := &struct {
		io.ReadCloser
		io.Writer
	}{ioutil.NopCloser(down), conn}

	bridge(dd, up)
}

func TestProxy(t *testing.T) {
	go func() {
		for {
			time.Sleep(2 * time.Second)
			f, _ := os.Create("heap.txt")
			pprof.Lookup("goroutine").WriteTo(f, 1)
			f.Close()
		}
	}()

	//p, _ := url.Parse("68.183.156.72:8080")
	//DefaultTransport.Proxy = http.ProxyURL(p)
	DefaultTransport.MaxConnsPerHost = 10
	DefaultTransport.MaxIdleConns = 10

	go func() {
		log.Println("hello")
		up := os.Getenv("UP")
		if up == "" {
			up = ":10001"
		}
		go http.ListenAndServe(":10000", &client{
			upstream: up,
		})

		ln, _ := Listen("tcp", ":10001")
		for {
			conn, _ := ln.Accept()
			go foo(conn)
		}
	}()

	//	Verbose = false
	select {}
}

func dnsIterQueryLocal(host string) net.IP {
	if idx := strings.LastIndex(host, ":"); idx > -1 {
		host = host[:idx]
	}
	if !strings.HasSuffix(host, ".") {
		host += "."
	}

	c := &dns.Client{Timeout: time.Millisecond * 100}
	m := &dns.Msg{}
	m.Id = dns.Id()
	m.RecursionDesired = false
	m.Question = []dns.Question{dns.Question{host, dns.TypeA, dns.ClassINET}}

	in, _, err := c.Exchange(m, "10.93.192.1:53")
	if err != nil || len(in.Answer) == 0 {
		vprint(err)
		return net.IPv4zero
	}

	switch a := in.Answer[0].(type) {
	case *dns.A:
		return a.A
	case *dns.CNAME:
		return dnsIterQueryLocal(a.Target)
	default:
		return net.IPv4zero
	}
}

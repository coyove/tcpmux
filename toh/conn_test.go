package toh

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"gitlab.com/ra_raven/go-common/rand"
)

var debugFlag = flag.Bool("debug", false, "")

func init() {
	flag.Parse()
	debug = *debugFlag
}

func TestClientConn(t *testing.T) {
	debug = true
	go func() {
		ln, _ := Listen("tcp", "127.0.0.1:13739")
		conn, _ := ln.Accept()
		p := [1]byte{}
		conn.Read(p[:])
		fmt.Println(p)
		p[0] = 2
		conn.Write(p[:])
	}()

	conn, _ := Dial("tcp", "127.0.0.1:13739")
	conn.Write([]byte{1})
	p := [1]byte{}
	conn.Read(p[:])
	fmt.Println(p)

	select {}
}

func TestHTTPServer(t *testing.T) {
	ready := make(chan bool)
	var ln net.Listener

	go func() {
		ln, _ = Listen("tcp", "127.0.0.1:13739")
		ready <- true
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(r.RequestURI[1:]))
		})
		http.Serve(ln, mux)
	}()

	num := 1
	client := http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				return Dial("tcp", "127.0.0.1:13739")
			},
		},
	}

	test := func(wg *sync.WaitGroup) {
		str := rand.FetchString(32)
		resp, err := client.Get("http://127.0.0.1:13739/" + str)

		if err != nil {
			panic(err.(*url.Error).Err)
		}

		buf, _ := ioutil.ReadAll(resp.Body)
		if string(buf) != str {
			panic(string(buf))
		}

		resp.Body.Close()
		wg.Done()
	}

	select {
	case <-ready:
	}

	go func() {
		for {
			time.Sleep(2 * time.Second)
			f, _ := os.Create("heap.txt")
			pprof.Lookup("goroutine").WriteTo(f, 1)
			fmt.Println("profile")
		}
	}()

	//debug = true
	start := time.Now()
	count := 0
	for {
		wg := &sync.WaitGroup{}
		if time.Now().Sub(start).Seconds() > 190 { // < 10 min, so we won't get killed by the go tester
			break
		}

		for i := 0; i < num*10; i++ {
			wg.Add(1)
			go test(wg)
		}
		wg.Wait()

		if count++; count%100 == 0 {
			//		log.Println(strings.Repeat("=", 20), count)
		}
		//logg.D(p.Count())
	}

	ln.Close()
}

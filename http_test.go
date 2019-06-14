package tcpmux

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, 16+r.Intn(10))
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(r.Intn(26)) + 'a'
	}
	return string(buf)
}

// go test -v -timeout 20m
func TestHTTPServer(t *testing.T) {
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	ready := make(chan bool)
	var ln net.Listener

	go func() {
		ln = getListerner()
		ready <- true
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			// http library tend to reuse the conn, but in this test we don't
			// h := w.(http.Hijacker)
			// conn, _, _ := h.Hijack()

			// res := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", len(r.RequestURI[1:]))
			// conn.Write([]byte(res + r.RequestURI[1:]))
			// conn.Close()

			w.Write([]byte(r.RequestURI[1:]))
		})
		http.Serve(ln, mux)
	}()

	num := 1
	p := NewDialer("127.0.0.1:13739", num)
	client := http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				s, err := p.Dial()
				// If we don't set OptErrWhenClosed,
				// for reading from a closed conn, we return (0, io.EOF)
				// for writing to a closed conn, we return (length of buf, nil)

				// s.(*Stream).SetStreamOpt(OptErrWhenClosed)
				return s, err
			},
		},
	}

	test := func(wg *sync.WaitGroup) {
		str := randomString()
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

		for i := 0; i < num*1; i++ {
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

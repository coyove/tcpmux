package tcpmux

import (
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"testing"
)

func getListerner() net.Listener {
	ln, err := Listen(":13739", true)
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
	d := NewDialer("127.0.0.1:13739", num)
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

func BenchmarkTCPServerMuxMultiWrite20(b *testing.B) {
	stringTransfer(20, b.N, true)
}

func BenchmarkTCPServerMuxMultiWrite10(b *testing.B) {
	stringTransfer(10, b.N, true)
}

func BenchmarkTCPServerMuxMultiWrite2(b *testing.B) {
	stringTransfer(2, b.N, true)
}

func BenchmarkTCPServerMuxMultiWrite1(b *testing.B) {
	stringTransfer(1, b.N, true)
}

func BenchmarkTCPServerMultiWrite(b *testing.B) {
	stringTransfer(0, b.N, false)
}

func BenchmarkHTTPServerMultiWrite(b *testing.B) {
	httpPoolStringTransfer(b.N)
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
			ln, _ = Listen(":13739", false)
		}
		ready <- true

		i := 0
		wg := sync.WaitGroup{}
		for {
			conn, err := ln.Accept()
			if err != nil {
				panic(err)
			}

			wg.Add(1)
			go func(conn net.Conn) {
				buf := make([]byte, 40)
				n, err := conn.Read(buf)
				if err != nil {
					panic(err)
				}

				buf = buf[:n]
				// idx := int(binary.BigEndian.Uint32(buf))
				// ln := int(binary.BigEndian.Uint32(buf[4:]))

				// strLock.Lock()
				// if string(buf[8:8+ln]) != strMap[idx] {
				// 	panic(fmt.Sprintf("%d: %v %v", len(strMap), string(buf[8:8+ln]), strMap[idx]))
				// }
				// delete(strMap, idx)
				// strLock.Unlock()
				if buf[0] != 1 {
					panic(buf[0])
				}
				// log.Println(time.Now().UnixNano()/1000, n)
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

	d := NewDialer("127.0.0.1:13739", num)
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

	// count = 1
	// total = loop
	for n := 0; n < total; n++ {
		wg := sync.WaitGroup{}
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) {
				conn, err := d.Dial()
				if err != nil {
					panic(err)
				}

				str := randomString()
				strLock.Lock()
				strMap[i] = str
				strLock.Unlock()

				buf := append([]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte(str)...)
				binary.BigEndian.PutUint32(buf, uint32(i))
				binary.BigEndian.PutUint32(buf[4:], uint32(len(str)))

				_, err = conn.Write([]byte{1})
				if err != nil {
					panic(err)
				}
				conn.Close()
				wg.Done()
				// log.Println("===")
			}(i)
		}
		wg.Wait()
		// time.Sleep(100 * time.Millisecond)
	}

	select {
	case <-exit:
	}
}

func httpPoolStringTransfer(loop int) {
	ready := make(chan bool)

	var ln net.Listener
	go func() {
		mux := http.NewServeMux()
		ln, _ = net.Listen("tcp", ":13739")
		ready <- true
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(r.RequestURI[1:]))
		})

		http.Serve(ln, mux)
	}()

	select {
	case <-ready:
	}

	client := &http.Client{}

	var total, count int
	if loop < 1000 {
		count = loop
		total = 1
	} else {
		count = 1000
		total = loop / count
	}

	for n := 0; n < total; n++ {
		wg := sync.WaitGroup{}
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) {
				str := randomString()

				req, _ := http.NewRequest("GET", "http://127.0.0.1:13739/"+str, nil)

				resp, err := client.Do(req)
				if err != nil {
					panic(err)
				}

				buf, _ := ioutil.ReadAll(resp.Body)
				if string(buf) != str {
					panic(string(buf))
				}
				resp.Body.Close()
				wg.Done()
			}(i)
		}
		wg.Wait()
	}

	ln.Close()
}

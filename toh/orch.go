package toh

import (
	"bytes"
	"encoding/binary"
	"time"
)

var orch chan *ClientConn

func init() {
	orch = make(chan *ClientConn, 256)
	//sched.Verbose = false

	go func() {
		for {
			conns := make(map[uint32]*ClientConn)
		READ:
			for {
				select {
				case c := <-orch:
					conns[c.idx] = c
				case <-time.After((time.Millisecond) * 50):
					break READ
				}
			}

			if len(conns) == 0 {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			var p bytes.Buffer
			var count int
			var lastconn *ClientConn

			for _, conn := range conns {
				if len(conn.write.buf) > 0 {
					// For connections with actual data waiting to be sent, send them directly
					go conn.sendWriteBuf()
					count++
					continue
				}

				binary.Write(&p, binary.BigEndian, conn.idx)
				lastconn = conn
			}

			if lastconn == nil {
				vprint("orch: send ", len(conns))
				continue
			}

			pingframe := frame{options: optPing, data: p.Bytes()}
			go func(pingframe frame, lastconn *ClientConn, conns map[uint32]*ClientConn) {
				resp, err := lastconn.send(pingframe)
				if err != nil {
					return
				}
				pcount := 0
				for {
					f, ok := parseframe(resp.Body, lastconn.read.blk)
					if !ok || f.idx == 0 {
						break
					}

					if c := conns[f.connIdx]; c != nil {
						c.write.respCh <- respNode{f: f}
						pcount++
					}
				}

				vprint("orch: pings: ", len(pingframe.data)/4, ", positives: ", pcount, "+", count)
				resp.Body.Close()
			}(pingframe, lastconn, conns)
		}
	}()
}

func orchSendWriteBuf(c *ClientConn) {
	select {
	case orch <- c:
	default:
		go c.sendWriteBuf()
	}
}

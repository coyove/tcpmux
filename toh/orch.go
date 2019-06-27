package toh

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/coyove/common/sched"
)

var orch chan *ClientConn

func init() {
	orch = make(chan *ClientConn, 256)
	sched.Verbose = false

	go func() {
		for {
			conns := make(map[uint64]*ClientConn)
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
			go func(pingframe frame, lastconn *ClientConn, conns map[uint64]*ClientConn) {
				resp, err := lastconn.send(pingframe)
				if err != nil {
					vprint("orch: send error: ", err)
					return
				}
				defer resp.Body.Close()

				pcount := 0
				f, ok := parseframe(resp.Body, lastconn.read.blk)
				if !ok || f.options != optPing {
					return
				}

				for i := 0; i < len(f.data); i += 10 {
					connState := binary.BigEndian.Uint16(f.data[i:])
					connIdx := binary.BigEndian.Uint64(f.data[i+2:])

					if c := conns[connIdx]; c != nil && !c.read.closed && c.read.err == nil {
						if connState == 2 {
							vprint(c, " the other side is closed")
							c.read.feedError(fmt.Errorf("use if closed connection"))
							c.Close()
						} else {
							pcount++
							go c.sendWriteBuf()
						}
					}
				}

				vprint("orch: pings: ", len(pingframe.data)/8, ", positives: ", pcount, "+", count)
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

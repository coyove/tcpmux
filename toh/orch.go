package toh

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/coyove/common/sched"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (d *Dialer) startOrch() {
	sched.Verbose = false

	go func() {
		for {
			conns := make(map[uint64]*ClientConn)
		READ:
			for {
				select {
				case c := <-d.orch:
					conns[c.idx] = c
				case <-time.After((time.Millisecond) * 50):
					break READ
				}
			}

			if len(conns) == 0 {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			var (
				p        bytes.Buffer
				count    int
				lastconn *ClientConn
			)

			for k, conn := range conns {
				if len(conn.write.buf) > 0 || conn.write.survey.lastIsPositive {
					// For connections with actual data waiting to be sent, send them directly
					go conn.sendWriteBuf()
					delete(conns, k)
					count++
					continue
				}

				binary.Write(&p, binary.BigEndian, conn.idx)
				lastconn = conn
			}

			if len(conns) <= 3 {
				for _, conn := range conns {
					count++
					go conn.sendWriteBuf()
				}
				lastconn = nil
			}

			if lastconn == nil {
				vprint("batch ping: 0, direct: ", count)
				continue
			}

			pingframe := frame{options: optPing, data: p.Bytes()}
			go func(pingframe frame, lastconn *ClientConn, conns map[uint64]*ClientConn) {
				resp, err := lastconn.send(pingframe)
				if err != nil {
					vprint("send error: ", err)
					return
				}
				defer resp.Body.Close()

				f, ok := parseframe(resp.Body, lastconn.read.blk)
				if !ok || f.options != optPing {
					return
				}

				positives := 0
				for i := 0; i < len(f.data); i += 10 {
					connState := binary.BigEndian.Uint16(f.data[i:])
					connIdx := binary.BigEndian.Uint64(f.data[i+2:])

					if c := conns[connIdx]; c != nil && !c.read.closed && c.read.err == nil {
						switch connState {
						case PING_CLOSED:
							vprint(c, " the other side is closed")
							c.read.feedError(errClosedConn)
							c.Close()
						case PING_OK_VOID:
							c.write.survey.lastIsPositive = false
						case PING_OK:
							positives++
							c.write.survey.lastIsPositive = true
							go c.sendWriteBuf()
						}
					}
				}

				vprint("batch ping: ", len(pingframe.data)/8, "(+", positives, "), direct: ", count)
				resp.Body.Close()
			}(pingframe, lastconn, conns)
		}
	}()
}

func (d *Dialer) orchSendWriteBuf(c *ClientConn) {
	select {
	case d.orch <- c:
	default:
		go c.sendWriteBuf()
	}
}

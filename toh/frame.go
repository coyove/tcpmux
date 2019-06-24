package toh

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/coyove/common/sched"
)

const (
	optSyncConnIdx = 1 << iota
	optHello
	optPing
	optClosed
)

type frame struct {
	idx     uint64
	connIdx uint32
	options byte
	data    []byte
	next    *frame
}

// connection id 8b | data idx 4b | data length 3b | option 1b |
func (f *frame) marshal(blk cipher.Block) io.Reader {
	buf := [16]byte{}
	binary.BigEndian.PutUint64(buf[:8], f.idx)
	binary.BigEndian.PutUint32(buf[8:], f.connIdx)

	if len(f.data) == 0 {
		f.data = make([]byte, 0, 16)
	}

	gcm, _ := cipher.NewGCM(blk)
	f.data = gcm.Seal(f.data[:0], buf[:12], f.data, nil)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(f.data))&0xffffff)
	buf[len(buf)-1] = f.options

	blk.Encrypt(buf[:], buf[:])

	if f.next == nil {
		return io.MultiReader(bytes.NewReader(buf[:]), bytes.NewReader(f.data))
	}
	return io.MultiReader(bytes.NewReader(buf[:]), bytes.NewReader(f.data), f.next.marshal(blk))
}

func (f *frame) size() int {
	if f.next == nil {
		return 16 + len(f.data)
	}
	return 16 + len(f.data) + f.next.size()
}

func parseframe(r io.ReadCloser, blk cipher.Block) (f frame, ok bool) {
	k := sched.ScheduleSync(func() {
		vprint("parseframe, waiting too long")
		r.Close()
	}, time.Now().Add(InactivePurge/2))
	defer k.Cancel()

	header := [16]byte{}
	if n, err := io.ReadAtLeast(r, header[:], len(header)); err != nil || n != len(header) {
		if err == io.EOF {
			ok = true
		}
		return
	}

	blk.Decrypt(header[:], header[:])

	datalen := int(binary.LittleEndian.Uint32(header[12:]) & 0xffffff)
	data := make([]byte, datalen)
	if n, err := io.ReadAtLeast(r, data, datalen); err != nil || n != datalen {
		return
	}

	gcm, err := cipher.NewGCM(blk)
	data, err = gcm.Open(nil, header[:12], data, nil)
	if err != nil {
		return
	}

	f.idx = binary.BigEndian.Uint64(header[:8])
	f.connIdx = binary.BigEndian.Uint32(header[8:12])
	f.data = data
	f.options = header[len(header)-1]
	return f, true
}

func (f frame) String() string {
	return fmt.Sprintf("<frame_%d_conn_%d_(%d)_len_%d>", f.idx, f.connIdx, f.options, len(f.data))
}

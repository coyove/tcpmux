package toh

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
)

const (
	optSyncIdx = 1 << iota
	optHello
)

type frame struct {
	idx     uint64
	connIdx uint32
	options byte
	data    []byte
	next    *frame
}

func connIdxToString(blk cipher.Block, idx uint32) string {
	p := [16]byte{}
	binary.BigEndian.PutUint32(p[:], idx)
	rand.Read(p[4:])
	copy(p[12:], "toh.")
	blk.Encrypt(p[:], p[:])
	return hex.EncodeToString(p[:])
}

func stringToConnIdx(blk cipher.Block, v string) (uint32, bool) {
	p, err := hex.DecodeString(v)
	if err != nil || len(p) != 16 {
		return 0, false
	}
	blk.Decrypt(p[:], p[:])
	return binary.BigEndian.Uint32(p), string(p[12:]) == "toh."
}

func (f *frame) marshal(blk cipher.Block) io.Reader {
	buf := [16]byte{}
	binary.BigEndian.PutUint64(buf[:8], f.idx)
	binary.BigEndian.PutUint32(buf[8:12], f.connIdx)

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

func parseframe(r io.Reader, blk cipher.Block) (f frame, ok bool) {
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

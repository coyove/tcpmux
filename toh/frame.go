package toh

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"
	"io"
)

const (
	OptResend = 1 << iota
)

type Frame struct {
	Idx     uint64
	ConnIdx uint32
	Options byte
	Data    []byte
}

func (f Frame) Marshal(blk cipher.Block) io.Reader {
	buf := [16]byte{}
	binary.BigEndian.PutUint64(buf[:8], f.Idx)
	binary.BigEndian.PutUint32(buf[8:12], f.ConnIdx)
	buf[len(buf)-1] = f.Options

	gcm, _ := cipher.NewGCM(blk)
	f.Data = gcm.Seal(f.Data[:0], buf[:12], f.Data, nil)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(f.Data))&0xffffff)

	blk.Encrypt(buf[:], buf[:])
	return io.MultiReader(bytes.NewReader(buf[:]), bytes.NewReader(f.Data))
}

func ParseFrame(r io.Reader, blk cipher.Block) (f Frame, ok bool) {
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

	f.Idx = binary.BigEndian.Uint64(header[:8])
	f.ConnIdx = binary.BigEndian.Uint32(header[8:12])
	f.Data = data
	f.Options = header[len(header)-1]
	return f, true
}

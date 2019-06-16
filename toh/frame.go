package toh

import (
	"bytes"
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

func (f Frame) Marshal() io.Reader {
	buf := [16]byte{}
	binary.BigEndian.PutUint64(buf[:8], f.Idx)
	binary.BigEndian.PutUint32(buf[8:12], f.ConnIdx)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(f.Data))&0xffffff)
	buf[len(buf)-1] = f.Options
	return io.MultiReader(bytes.NewReader(buf[:]), bytes.NewReader(f.Data))
}

func ParseFrame(r io.Reader) (f Frame, ok bool) {
	header := [16]byte{}
	if n, err := io.ReadAtLeast(r, header[:], len(header)); err != nil || n != len(header) {
		if err == io.EOF {
			ok = true
		}
		return
	}

	datalen := int(binary.LittleEndian.Uint32(header[12:]) & 0xffffff)
	data := make([]byte, datalen)
	if n, err := io.ReadAtLeast(r, data, datalen); err != nil || n != datalen {
		return
	}

	f.Idx = binary.BigEndian.Uint64(header[:8])
	f.ConnIdx = binary.BigEndian.Uint32(header[8:12])
	f.Data = data
	f.Options = header[len(header)-1]
	return f, true
}

package toh

import (
	"bytes"
	"encoding/binary"
	"io"
)

const (
	OptHello = 1 << iota
	OptAck
)

type Frame struct {
	Idx       uint64
	StreamIdx uint64
	Options   byte
	Data      []byte
}

func (f Frame) Marshal() io.Reader {
	buf := [24]byte{}
	binary.BigEndian.PutUint64(buf[:8], f.Idx)
	binary.BigEndian.PutUint64(buf[8:16], f.StreamIdx)
	binary.BigEndian.PutUint32(buf[16:], uint32(len(f.Data)))
	buf[23] = f.Options
	return io.MultiReader(bytes.NewReader(buf[:]), bytes.NewReader(f.Data))
}

func ParseFrame(r io.Reader) (f Frame, ok bool) {
	header := [24]byte{}
	if n, err := io.ReadAtLeast(r, header[:], len(header)); err != nil || n != len(header) {
		if err == io.EOF {
			ok = true
		}
		return
	}

	datalen := int(binary.BigEndian.Uint32(header[16:]))
	data := make([]byte, datalen)
	if n, err := io.ReadAtLeast(r, data, datalen); err != nil || n != datalen {
		return
	}

	f.Idx = binary.BigEndian.Uint64(header[:8])
	f.StreamIdx = binary.BigEndian.Uint64(header[8:16])
	f.Data = data
	f.Options = header[23]
	return f, true
}

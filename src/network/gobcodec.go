package network

import (
	"raft_kv_backend/gob_check"
	"io"
	"reflect"
)

type GobEncoder struct {
	*gob_check.LabEncoder
}

func NewGobEncoder(w io.Writer) *GobEncoder {
	return &GobEncoder{
		gob_check.NewEncoder(w),
	}
}

func (g *GobEncoder) Encode(e interface{}) error {
	return g.LabEncoder.Encode(e)
}

func (g *GobEncoder) EncodeValue(value reflect.Value) error {
	return g.LabEncoder.EncodeValue(value)
}

type GobDecoder struct {
	*gob_check.LabDecoder
}

func NewGobDecoder(r io.Reader) *GobDecoder {
	return &GobDecoder{
		gob_check.NewDecoder(r),
	}
}

func (g *GobDecoder) Decode(e interface{}) error {
	return g.LabDecoder.Decode(e)
}

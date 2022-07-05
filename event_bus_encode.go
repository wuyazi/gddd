package gddd

import (
	"encoding/json"
	"errors"
)

type EventDecoder interface {
	RAW() (raw []byte)
	Decode(v interface{}) (err error)
}

func NewRawEventDecoder(raw []byte) *RawEventDecoder {
	return &RawEventDecoder{
		raw: raw,
	}
}

type RawEventDecoder struct {
	raw []byte
}

func (d *RawEventDecoder) RAW() (raw []byte) {
	raw = d.raw
	return
}

func (d *RawEventDecoder) Decode(v interface{}) (err error) {
	err = errors.New("event decoder failed, it is native, please call decoder.RAW to get event bytes")
	return
}

func NewJsonEventDecoder(raw []byte) *JsonEventDecoder {
	return &JsonEventDecoder{
		raw: raw,
	}
}

type JsonEventDecoder struct {
	raw json.RawMessage
}

func (d *JsonEventDecoder) RAW() (raw []byte) {
	raw = d.raw
	return
}

func (d *JsonEventDecoder) Decode(v interface{}) (err error) {
	err = json.Unmarshal(d.raw, v)
	return
}

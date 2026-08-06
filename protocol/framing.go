package protocol

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

const DefaultMaxFrameSize = 1 << 20

type Encoder struct {
	w       io.Writer
	MaxSize int
}

func NewEncoder(w io.Writer) *Encoder { return &Encoder{w: w, MaxSize: DefaultMaxFrameSize} }
func (e *Encoder) Write(value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) == 0 || data[0] != '{' {
		return fmt.Errorf("frame must be a JSON object: %w", ErrInvalidFrame)
	}
	if e.MaxSize > 0 && len(data) > e.MaxSize {
		return fmt.Errorf("frame size %d exceeds %d: %w", len(data), e.MaxSize, ErrFrameTooLarge)
	}
	_, err = e.w.Write(append(data, '\n'))
	return err
}

type Decoder struct {
	r       *bufio.Reader
	MaxSize int
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: bufio.NewReader(r), MaxSize: DefaultMaxFrameSize}
}
func (d *Decoder) ReadFrame() ([]byte, error) {
	var frame []byte
	for {
		part, err := d.r.ReadBytes('\n')
		frame = append(frame, part...)
		if d.MaxSize > 0 && len(frame) > d.MaxSize+1 {
			return nil, fmt.Errorf("frame exceeds %d bytes: %w", d.MaxSize, ErrFrameTooLarge)
		}
		if err != nil {
			if err == io.EOF && len(frame) == 0 {
				return nil, io.EOF
			}
			if err != io.EOF {
				return nil, err
			}
		}
		if len(frame) == 0 || frame[len(frame)-1] == '\n' || err == io.EOF {
			break
		}
	}
	frame = bytes.TrimSuffix(frame, []byte{'\n'})
	frame = bytes.TrimSuffix(frame, []byte{'\r'})
	if len(frame) == 0 {
		return nil, fmt.Errorf("empty frame: %w", ErrMalformedFrame)
	}
	if d.MaxSize > 0 && len(frame) > d.MaxSize {
		return nil, fmt.Errorf("frame exceeds %d bytes: %w", d.MaxSize, ErrFrameTooLarge)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(frame, &object); err != nil || object == nil {
		return nil, fmt.Errorf("invalid JSON frame: %w", ErrMalformedFrame)
	}
	return frame, nil
}

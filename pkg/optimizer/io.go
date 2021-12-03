//
// DISCLAIMER
//
// Copyright 2016-2021 ArangoDB GmbH, Cologne, Germany
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright holder is ArangoDB GmbH, Cologne, Germany
//

package optimizer

import (
	"errors"
	"io"
)

const MaxBufferSize = 32 * 1024 * 1024
const MaxBufferParts = MaxBufferSize / 8

type ReadHandler func(p Process, buffer []byte, parts [][]byte)

func ReadSizeDelimit(handler Handler, in io.Reader, size int, h ReadHandler) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		delimBuff := make([][]byte, MaxBufferParts)
		origBuff := make([]byte, MaxBufferSize)
		offset := 0

		for {
			n, err := in.Read(origBuff[offset:])
			if err != nil {
				if !errors.Is(err, io.EOF) {
					p.Emit(err)
					return
				}

				// We need to push last message
				if offset > 0 {
					delimBuff[0] = origBuff[0:offset]
					h(p, origBuff[0:offset], delimBuff[0:1])
				}
				return
			}

			buff := origBuff[:n+offset]
			a, b := sizeDelimit(buff, delimBuff, size)

			h(p, buff[:a], delimBuff[:b])

			if len(buff) > a {
				for id, b := range buff[a:] {
					origBuff[id] = b
				}
				offset = len(buff) - a
			} else {
				offset = 0
			}
		}
	}()

	return p
}

func ReadL(handler Handler, in io.Reader, h ReadHandler) Process {
	return ReadDelimit(handler, in, '\n', h)
}

func ReadDelimit(handler Handler, in io.Reader, delimitByte byte, h ReadHandler) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		delimBuff := make([][]byte, MaxBufferParts)
		origBuff := make([]byte, MaxBufferSize)
		offset := 0

		for {
			n, err := in.Read(origBuff[offset:])
			if err != nil {
				if !errors.Is(err, io.EOF) {
					p.Emit(err)
					return
				}

				// We need to push last message
				if offset > 0 {
					delimBuff[0] = origBuff[0:offset]
					h(p, origBuff[0:offset], delimBuff[0:1])
				}
				return
			}

			buff := origBuff[:n+offset]
			a, b := delimit(buff, delimBuff, delimitByte)

			h(p, buff[:a], delimBuff[:b])

			if len(buff) > a {
				for id, b := range buff[a:] {
					origBuff[id] = b
				}
				offset = len(buff) - a
			} else {
				offset = 0
			}
		}
	}()

	return p
}

func sizeDelimit(in []byte, out [][]byte, size int) (int, int) {
	offset := 0
	id := size
	last := 0
	for {
		if offset == len(out) || id >= len(in) {
			return last, offset
		}

		out[offset] = in[last : last+size]

		last = id
		id += size
		offset++
	}
}

func delimit(in []byte, out [][]byte, delimit byte) (int, int) {
	offset := 0
	id := 0
	last := 0
	for {
		if offset == len(out) || id >= len(in) {
			return last, offset
		}

		if in[id] == delimit {
			// We are on delimiter
			if id == 0 {
				id++
				continue
			}

			out[offset] = in[last:id]

			offset++
			id += 2
			last = id - 1
			if id > len(in) {
				id = len(in)
			}
			continue
		}

		id++
	}
}

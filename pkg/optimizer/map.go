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
	"bufio"
	"encoding/binary"
	"os"
	"runtime"
	"strconv"
	"time"
)

func MapVertexesAndEdges(handler Handler, vertexes *os.File, in, tmpA, tmpB, out string, size, threads int) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		last, next := in, tmpA

		m := map[string]int{}
		id := 0
		fl := 0
		iterations := 1

		vertexCount := DiscoverL(handler, p, vertexes.Name(), "vertexes")
		edgeCount := DiscoverL(handler, p, in, "edges")

		edgeCurrent := 0

		t := p.Task(5*time.Second, func(state string, duration time.Duration) {
			if edgeCurrent == edgeCount || edgeCurrent == 0 {
				WithMemory(p.Info()).Msgf("%s (%s): Mapping of vertexes & edges (%3.4f%%) - create key mapping (%3.4f%%)", state, duration.String(), float64(100)*(float64(id)/float64(vertexCount)), float64(100)*(float64(fl)/float64(size)))
			} else {
				WithMemory(p.Info()).Msgf("%s (%s): Mapping of vertexes & edges (%3.4f%%) - iteration %d (%3.4f%%)", state, duration.String(), float64(100)*(float64(id)/float64(vertexCount)), iterations, float64(100)*(float64(edgeCurrent)/float64(edgeCount)))
			}
		})

		defer t.Done()

		ReadL(handler, vertexes, func(p Process, buffer []byte, parts [][]byte) {
			for q := range parts {
				m[string(parts[q])] = id + q
			}

			id += len(parts)
			fl += len(parts)

			if fl >= size {
				fl = 0

				edgeCurrent = 0
				mapVertexesBatch(handler, m, last, next, &edgeCurrent, threads).Wait()
				iterations++
				m = map[string]int{}

				if next == tmpA {
					last, next = tmpA, tmpB
				} else {
					last, next = tmpB, tmpA
				}

				runtime.GC()
			}
		}).Wait()

		if fl > 0 {
			edgeCurrent = 0
			mapVertexesBatch(handler, m, last, next, &edgeCurrent, threads).Wait()

			if next == tmpA {
				last, next = tmpA, tmpB
			} else {
				last, next = tmpB, tmpA
			}
		}

		mapEdgesToInt(handler, last, out).Wait()
	}()

	return p
}

func mapVertexesBatch(handler Handler, s map[string]int, in, out string, edgeCurrent *int, threads int) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		fin, err := os.OpenFile(in, os.O_RDONLY, 0644)
		if err != nil {
			p.Emit(err)
			return
		}

		defer fin.Close()

		fout, err := os.OpenFile(out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			p.Emit(err)
			return
		}

		defer fout.Close()

		writter := bufio.NewWriterSize(fout, MaxBufferSize)

		ReadL(handler, fin, func(sp Process, buffer []byte, parts [][]byte) {
			np := make([][]byte, len(parts))
			RunInThread(threads, len(parts), func(id int) {
				np[id] = parts[id]
				if len(parts[id]) == 0 {
					return
				}

				if parts[id][0] == 0 {
					return
				}

				if z, ok := s[string(parts[id])]; ok {
					np[id] = append([]byte{0}, strconv.Itoa(z)...)
					return
				}
			})
			for id := range np {
				if _, err := writter.Write(np[id]); err != nil {
					p.Emit(err)
				}
				if err := writter.WriteByte('\n'); err != nil {
					p.Emit(err)
				}
			}
			*edgeCurrent += len(parts)
		}).Wait()

		if err := writter.Flush(); err != nil {
			p.Emit(err)
		}
	}()

	return p
}

func mapEdgesToInt(handler Handler, in, out string) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		fin, err := os.OpenFile(in, os.O_RDONLY, 0644)
		if err != nil {
			p.Emit(err)
			return
		}

		defer p.DeferEmit(fin.Close)

		fout, err := os.OpenFile(out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			p.Emit(err)
			return
		}

		defer p.DeferEmit(fout.Close)

		writter := bufio.NewWriterSize(fout, MaxBufferSize)

		e := make([]byte, 8)

		ids := 0

		ReadL(handler, fin, func(sp Process, buffer []byte, parts [][]byte) {
			for id := range parts {
				ids++
				n, err := strconv.Atoi(string(parts[id][1:]))
				if err != nil {
					p.Emit(err)
					return
				}

				for id := 0; id < 8; id++ {
					e[id] = 0
				}

				binary.PutVarint(e, int64(n))

				if _, err := writter.Write(e); err != nil {
					p.Emit(err)
				}
			}
		}).Wait()

		if err := writter.Flush(); err != nil {
			p.Emit(err)
		}
	}()

	return p
}

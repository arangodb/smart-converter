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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type Mapping struct {
	A, B int64
}

func RunVertexTranslation(handler Handler, optimized, vertexes *os.File, vertexesOut io.Writer, threads int) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		vertexCount := DiscoverL(handler, p, vertexes.Name(), "vertexes")

		weights := GetWeights(handler, p, optimized, vertexCount)

		runtime.GC()

		RemapVertexes(handler, weights, vertexes, vertexesOut, vertexCount, threads).Wait()
	}()

	return p
}

func RunEdgesTranslation(handler Handler, optimized, vertexes, edges, edgeMap *os.File, edgesOut io.Writer, threads int) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		vertexCount := DiscoverL(handler, p, vertexes.Name(), "vertexes")
		edgesCount := DiscoverL(handler, p, edges.Name(), "edges")

		mapping := GetEdgeWeights(handler, p, optimized, edgeMap, vertexCount, edgesCount)

		runtime.GC()

		RemapEdges(handler, mapping, edges, edgesOut, edgesCount, threads).Wait()
	}()

	return p
}

func GetEdgeWeights(handler Handler, process ProcessInt, mapping, edges io.Reader, vertexCount, edgesCount int) []Mapping {
	weights := GetWeights(handler, process, mapping, vertexCount)

	edgeMap := make([]Mapping, edgesCount)
	index := 0

	t := process.Task(time.Second, func(state string, duration time.Duration) {
		WithMemory(process.Info()).Msgf("%s (%s): Init of mapping (%d)", state, duration.String(), len(edgeMap))
	})
	defer t.Done()

	ReadMapping(handler, edges, func(mapping []Mapping) {
		for mid := range mapping {
			edgeMap[index+mid].A = weights[mapping[mid].A]
			edgeMap[index+mid].B = weights[mapping[mid].B]
		}
		index += len(mapping)
	}).Wait()

	defer runtime.GC()

	return edgeMap
}

func GetWeights(handler Handler, process ProcessInt, mapping io.Reader, vertexCount int) []int64 {
	m := make([]int64, vertexCount)
	index := 0

	t := process.Task(time.Second, func(state string, duration time.Duration) {
		WithMemory(process.Info()).Msgf("%s (%s): Init of weights (%d)", state, duration.String(), len(m))
	})
	defer t.Done()
	ReadMapping(handler, mapping, func(mapping []Mapping) {
		for mid := range mapping {
			m[index+mid] = mapping[mid].A
		}
		index += len(mapping)
	}).Wait()

	defer runtime.GC()

	return m
}

func RemapEdges(handler Handler, edgeWeights []Mapping, in io.Reader, out io.Writer, edgesCount, threads int) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		writter := bufio.NewWriterSize(out, MaxBufferSize)

		index := 0

		t := p.Task(time.Second, func(state string, duration time.Duration) {
			WithMemory(p.Info()).Msgf("%s (%s): Transforming edges (%3.4f%%)", state, duration.String(), float64(100)*(float64(index)/float64(edgesCount)))
		})
		defer t.Done()

		ReadL(handler, in, func(sp Process, buffer []byte, parts [][]byte) {
			out := make([][]byte, len(parts))

			RunInThread(threads, len(parts), func(id int) {
				var q map[string]interface{}

				if err := json.Unmarshal(parts[id], &q); err != nil {
					p.Emit(err)
					return
				}

				delete(q, "_rev")
				delete(q, "_id")
				delete(q, "_key")

				if from, ok := q["_from"]; ok {
					if s, ok := from.(string); ok {
						parts := strings.Split(s, "/")
						if len(parts) == 2 {
							q["_from"] = fmt.Sprintf("%s/%d:%s", "entities2", edgeWeights[index+id].A, parts[1])
						}
					}
				}

				if from, ok := q["_to"]; ok {
					if s, ok := from.(string); ok {
						parts := strings.Split(s, "/")
						if len(parts) == 2 {
							q["_to"] = fmt.Sprintf("%s/%d:%s", "entities2", edgeWeights[index+id].B, parts[1])
						}
					}
				}

				if d, err := json.Marshal(q); err != nil {
					p.Emit(err)
				} else {
					out[id] = d
				}
			})

			index += len(parts)

			for id := range out {
				if _, err := writter.Write(out[id]); err != nil {
					p.Emit(err)
				}
				if err := writter.WriteByte('\n'); err != nil {
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

func RemapVertexes(handler Handler, m []int64, in io.Reader, out io.Writer, vertexCount, threads int) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		writter := bufio.NewWriterSize(out, MaxBufferSize)

		index := 0

		t := p.Task(time.Second, func(state string, duration time.Duration) {
			WithMemory(p.Info()).Msgf("%s (%s): Transforming vertexes (%3.4f%%)", state, duration.String(), float64(100)*(float64(index)/float64(vertexCount)))
		})
		defer t.Done()

		ReadL(handler, in, func(sp Process, buffer []byte, parts [][]byte) {
			out := make([][]byte, len(parts))

			RunInThread(threads, len(parts), func(id int) {
				var q map[string]interface{}

				if err := json.Unmarshal(parts[id], &q); err != nil {
					p.Emit(err)
					return
				}

				key, ok := q["_key"]
				if !ok {
					p.Emit(errors.New("missing _key attribute"))
					return
				}

				q["serial_number"] = key

				weight := int64(index + id)
				if index+id < len(m) {
					weight = m[index+id]
				}

				q["smart"] = fmt.Sprintf("%d", weight)
				q["_key"] = fmt.Sprintf("%d:%s", weight, key)

				delete(q, "_rev")
				delete(q, "_id")

				if d, err := json.Marshal(q); err != nil {
					p.Emit(err)
				} else {
					out[id] = d
				}
			})

			index += len(out)

			for id := range out {
				if _, err := writter.Write(out[id]); err != nil {
					p.Emit(err)
				}
				if err := writter.WriteByte('\n'); err != nil {
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

func RunOptimization(handler Handler, in *os.File, out io.Writer) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		var max int64 = 0
		readed := 0

		t := p.Task(time.Second, func(state string, duration time.Duration) {
			WithMemory(p.Info()).Msgf("%s (%s): Init of mapped edges (%d)", state, duration.String(), readed)
		})

		ReadMapping(handler, in, func(m []Mapping) {
			for id := range m {
				if m[id].A > max {
					max = m[id].A
				}
				if m[id].B > max {
					max = m[id].B
				}
			}
			readed += len(m)
		}).Wait()

		t.Done()

		state := GenerateMapping(max + 1)

		current := 0
		iteration := 0

		t = p.Task(time.Second, func(state string, duration time.Duration) {
			WithMemory(p.Info()).Msgf("%s (%s): Running optimization - iteration %d (%3.4f%%)", state, duration.String(), iteration, float64(100)*(float64(current)/float64(readed)))
		})
		defer t.Done()

		for {
			if _, err := in.Seek(0, 0); err != nil {
				p.Emit(err)
				return
			}

			changed := 0

			iteration++
			current = 0

			ReadMapping(handler, in, func(m []Mapping) {
				for id := range m {
					if q := state[m[id].B].B; q != -1 && q != m[id].A {
						continue
					}

					min := Min(state[m[id].A].A, state[m[id].B].A)

					if state[m[id].B].A != min || state[m[id].A].A != min || state[m[id].B].B != m[id].A {
						changed++
						state[m[id].B].A, state[m[id].B].B = min, m[id].A
						state[m[id].A].A = min
					}
				}
				current += len(m)
			}).Wait()

			if changed == 0 {
				break
			}
		}

		WriteMapping(handler, out, state).Wait()
	}()

	return p
}

func Min(x ...int64) int64 {
	if len(x) == 0 {
		return -1
	}

	z := x[0]

	for i := 1; i < len(x); i++ {
		if x[i] < z {
			z = x[i]
		}
	}

	return z
}

func ReadMapping(handler Handler, in io.Reader, mappingCallback func(m []Mapping)) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		m := make([]Mapping, MaxBufferParts)

		ReadSizeDelimit(handler, in, 16, func(sp Process, buffer []byte, parts [][]byte) {
			for id := range parts {
				if len(parts[id]) != 16 {
					p.Emit(errors.Errorf("Error while getting data"))
					continue
				}

				if l, n := binary.Varint(parts[id][0:8]); n <= 0 {
					p.Emit(errors.Errorf("Unable to read int"))
				} else {
					m[id].A = l
				}

				if l, n := binary.Varint(parts[id][8:16]); n <= 0 {
					p.Emit(errors.Errorf("Unable to read int"))
				} else {
					m[id].B = l
				}
			}

			mappingCallback(m[:len(parts)])
		}).Wait()
	}()

	return p
}

func WriteMapping(handler Handler, out io.Writer, data []Mapping) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		writter := bufio.NewWriterSize(out, MaxBufferSize)

		i := make([]byte, 16)

		for id := range data {
			for j := 0; j < 16; j++ {
				i[j] = 0
			}

			binary.PutVarint(i[0:8], data[id].A)
			binary.PutVarint(i[8:16], data[id].B)

			if _, err := writter.Write(i); err != nil {
				p.Emit(err)
			}
		}

		if err := writter.Flush(); err != nil {
			p.Emit(err)
		}
	}()

	return p
}

func GenerateMapping(size int64) []Mapping {
	part := make([]Mapping, size)

	for i := int64(0); i < size; i++ {
		part[i] = Mapping{
			A: i,
			B: -1,
		}
	}

	return part
}

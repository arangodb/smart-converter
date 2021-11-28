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
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

type Mapping struct {
	A, B int64
}

func RunTranslation(optimized, vertexes, edges, edgeMap io.Reader, vertexesOut, edgesOut io.Writer, threads int) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		m := GetMapping(errs, optimized)

		index := 0

		var wg sync.WaitGroup

		wg.Add(2)

		go func() {
			defer wg.Done()

			// Lets run over vertexes
			vertexesIn, verr := ReadNL(vertexes)
			defer PushErrors(verr, errs)()

			outData := make(chan [][]byte)
			defer PushErrors(WriteNL(vertexesOut, outData), errs)()

			for v := range vertexesIn {
				out := make([][]byte, len(v))
				RunInThread(threads, len(v), func(id int) {
					var q map[string]interface{}

					if err := json.Unmarshal(v[id], &q); err != nil {
						errs <- err
						return
					}

					key, ok := q["_key"]
					if !ok {
						errs <- errors.New("missing _key attribute")
					}

					q["serial_number"] = key

					weight := int64(index + id)
					if index+id < len(m) {
						weight = m[index+id].A
					}

					q["smart"] = fmt.Sprintf("%d", weight)
					q["_key"] = fmt.Sprintf("%d:%s", weight, key)

					delete(q, "_rev")
					delete(q, "_id")

					if d, err := json.Marshal(q); err != nil {
						errs <- err
					} else {
						out[id] = d
					}
				})

				outData <- out
				index += len(v)
			}

			close(outData)
		}()


		go func() {
			defer wg.Done()

			// Iterate over edges
			eOut, eErr := ReadMapping(edgeMap)
			PushErrors(eErr, errs)

			outData := make(chan [][]byte)
			defer PushErrors(WriteNL(edgesOut, outData), errs)()

			edgesIn, verr := ReadNL(edges)
			defer PushErrors(verr, errs)()

			for v := range edgesIn {
				edgeMap, ok := <-eOut
				if !ok {
					panic("")
				}

				if len(v) != len(edgeMap) {
					panic("")
				}

				out := make([][]byte, len(v))
				RunInThread(threads, len(v), func(id int) {
					var q map[string]interface{}

					if err := json.Unmarshal(v[id], &q); err != nil {
						errs <- err
						return
					}
					delete(q, "_rev")
					delete(q, "_id")
					delete(q, "_key")

					if from, ok := q["_from"]; ok {
						if s, ok := from.(string); ok {
							parts := strings.Split(s, "/")
							if len(parts) == 2 {
								q["_from"] = fmt.Sprintf("%s/%d:%s", "entities2", m[edgeMap[id].A].A, parts[1])
							}
						}
					}

					if from, ok := q["_to"]; ok {
						if s, ok := from.(string); ok {
							parts := strings.Split(s, "/")
							if len(parts) == 2 {
								q["_to"] = fmt.Sprintf("%s/%d:%s", "entities2", m[edgeMap[id].B].A, parts[1])
							}
						}
					}

					if d, err := json.Marshal(q); err != nil {
						errs <- err
					} else {
						out[id] = d
					}
				})
				outData <- out
			}

			close(outData)
		}()

		wg.Wait()
	}()

	return errs
}

func RunOptimization(in *os.File, out io.Writer, buffer int) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		m, merr := ReadMapping(in)
		defer PushErrors(merr, errs)()

		max := int64(0)

		for q := range m {
			for _, v := range q {
				if v.A > max {
					max = v.A
				}
				if v.B > max {
					max = v.B
				}
			}
		}

		state := GenerateMapping(max + 1)

		for {
			if _, err := in.Seek(0, 0); err != nil {
				errs <- err
				break
			}

			m, merr := ReadMapping(in)
			defer PushErrors(merr, errs)()

			changed := 0

			println("Iteration")

			for q := range m {
				for _, v := range q {
					if q := state[v.B].B; q != -1 && q != v.A {
						continue
					}

					min := Min(state[v.A].A, state[v.B].A)

					if state[v.B].A != min || state[v.A].A != min || state[v.B].B != v.A {
						changed++
						state[v.B].A, state[v.B].B = min, v.A
						state[v.A].A = min
					}
				}
			}

			println("Iteration done", changed)

			if changed == 0 {
				break
			}
		}

		data := make(chan []Mapping)
		defer PushErrors(WriteMapping(out, data, buffer), errs)()

		for id := 0; id < len(state); id += buffer {
			if id+buffer > len(state) {
				data <- state[id:]
			} else {
				data <- state[id : id+buffer]
			}
		}

		close(data)
	}()

	return errs
}

func GenerateMapping(size int64) []Mapping {
	q := make([]Mapping, size)

	for i := int64(0); i < size; i++ {
		q[i].A = i
		q[i].B = -1
	}

	return q
}

func WriteMapping(out io.Writer, in <-chan []Mapping, buffer int) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		data := make(chan [][]byte, buffer)
		defer PushErrors(Write(out, data), errs)()

		for q := range in {
			z := make([][]byte, len(q))

			for id := range z {
				i := make([]byte, 16)

				binary.PutVarint(i[0:8], q[id].A)
				binary.PutVarint(i[8:16], q[id].B)

				z[id] = i
			}

			data <- z
		}

		close(data)

	}()

	return errs
}

func GetMapping(errs chan<- error, in io.Reader) []Mapping {
	var m []Mapping

	mc, e := ReadMapping(in)
	defer PushErrors(e, errs)()

	for q := range mc {
		m = append(m, q...)
	}

	return m
}

func ReadMapping(in io.Reader) (<-chan []Mapping, <-chan error) {
	errs := make(chan error, 32)
	mappings := make(chan []Mapping)

	go func() {
		defer close(errs)
		defer close(mappings)

		scanner := bufio.NewReaderSize(in, 4*1024*1024)

		m := make([]Mapping, IOBufferSize)
		id := 0

		b := make([]byte, 16)

		for {
			n, err := scanner.Read(b)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				errs <- err
				continue
			}

			if n != 16 {
				errs <- errors.New("invalid size of read")
				continue
			}

			if r, b := binary.Varint(b[0:8]); b == 0 {
				errs <- errors.New("invalid size of read")
				continue
			} else {
				m[id].A = r
			}

			if r, b := binary.Varint(b[8:16]); b == 0 {
				errs <- errors.New("invalid size of read")
				continue
			} else {
				m[id].B = r
			}

			id++

			if id == IOBufferSize {
				mappings <- m

				m = make([]Mapping, IOBufferSize)
				id = 0
			}
		}

		if id > 0 {
			mappings <- m[0:id]
		}
	}()

	return mappings, errs
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

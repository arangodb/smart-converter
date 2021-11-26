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
	"encoding/binary"
	"io"
	"os"
	"strconv"
)

func MapVertexesAndEdges(p Progress, vertexes io.Reader, in, tmpA, tmpB, out string, buffer int, threads int) <-chan error {
	errs := make(chan error)

	go func() {
		defer close(errs)

		last, next := in, tmpA

		vertexes, verr := ReadVertex(p, vertexes, buffer)
		go PushErrors(verr, errs)()

		for m := range vertexToMap(vertexes) {

			if err := mapVertexesBatch(p, errs, m, last, next, buffer/512, threads); err != nil {
				errs <- err
			}

			if next == tmpA {
				last, next = tmpA, tmpB
			} else {
				last, next = tmpB, tmpA
			}
		}

		if err := mapEdgesToInt(p, errs, last, out); err != nil {
			errs <- err
		}
	}()

	return errs
}

func vertexToMap(in <-chan []Vertex) <-chan map[string]int {
	out := make(chan map[string]int, 4)

	go func() {
		defer close(out)

		offset := 0

		for v := range in {
			m := map[string]int{}
			for id := range v {
				m[string(v[id])] = id + offset
			}

			offset += len(v)

			out <- m
		}
	}()

	return out
}

func mapVertexesBatch(p Progress, errs chan<- error, s map[string]int, in, out string, batchPerThread, threads int) error {
	fin, err := os.OpenFile(in, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer fin.Close()

	fout, err := os.OpenFile(out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer fout.Close()

	din, derr := ReadNL(p, fin, batchPerThread*threads)
	defer PushErrors(derr, errs)()

	dout := make(chan [][]byte, 4)
	defer PushErrors(WriteNL(p, fout, dout), errs)()

	for d := range din {
		r := make([][]byte, len(d))
		RunInThread(threads, len(d), func(id int) {
			r[id] = d[id]

			if len(d[id]) == 0 {
				return
			}

			if d[id][0] == 0 {
				return
			}

			if z, ok := s[string(d[id])]; ok {
				r[id] = append([]byte{0}, strconv.Itoa(z)...)
				return
			}
		})
		dout <- r
	}

	close(dout)

	return nil
}

func mapEdgesToInt(p Progress, errs chan<- error, in, out string) error {
	fin, err := os.OpenFile(in, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer fin.Close()

	fout, err := os.OpenFile(out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer fout.Close()

	din, derr := ReadNL(p, fin, 16*1024)
	defer PushErrors(derr, errs)()

	dout := make(chan [][]byte)
	defer PushErrors(Write(p, fout, dout), errs)()

	for d := range din {
		r := make([][]byte, len(d))

		for id := range d {
			n, err := strconv.Atoi(string(d[id][1:]))
			if err != nil {
				errs <- err
			}

			e := make([]byte, 8)

			binary.PutVarint(e, int64(n))

			r[id] = e
		}

		dout <- r
	}

	close(dout)

	return nil
}

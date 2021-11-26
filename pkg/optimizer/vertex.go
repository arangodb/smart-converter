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
	"io"
)

type EdgeVertex struct {
	From, To Vertex
}

type Vertex string

func ExtractEdgeVertexFromEdge(p Progress, in io.Reader, out io.Writer, buffer, threads int) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)
		defer p.Done()

		vertexes, vertexErr := ReadEdgesFromEdgeDocuments(p, in, buffer, threads)
		defer PushErrors(vertexErr, errs)()

		defer PushErrors(WriteEdgeVertex(p, out, vertexes), errs)()
	}()
	return errs
}

func ExtractVertexFromDocuments(p Progress, in io.Reader, out io.Writer, buffer, threads int) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)
		defer p.Done()

		vertexes, vertexErr := ReadVertexFromVertexDocuments(p, in, buffer, threads)
		defer PushErrors(vertexErr, errs)()

		defer PushErrors(WriteVertex(p, out, vertexes), errs)()
	}()
	return errs
}

func WriteEdgeVertex(p Progress, writer io.Writer, docs <-chan []EdgeVertex) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		data := make(chan [][]byte)
		ein := WriteNL(p, writer, data)
		defer PushErrors(ein, errs)()

		for in := range docs {
			q := make([][]byte, len(in)*2)

			for id := range in {
				q[id*2] = []byte(in[id].From)
				q[id*2+1] = []byte(in[id].To)
			}

			p.Job("WRITE_EDGE_VERTEX").Add(len(q))
			data <- q
		}

		close(data)
	}()

	return errs
}

func WriteVertex(p Progress, writer io.Writer, docs <-chan []Vertex) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		data := make(chan [][]byte)
		ein := WriteNL(p, writer, data)
		defer PushErrors(ein, errs)()

		for in := range docs {
			q := make([][]byte, len(in))

			for id := range in {
				q[id] = []byte(in[id])
			}

			p.Job("WRITE_VERTEX").Add(len(q))
			data <- q
		}

		close(data)
	}()

	return errs
}

func ReadVertex(p Progress, in io.Reader, buffer int) (<-chan []Vertex, <-chan error) {
	errs := make(chan error, 32)
	vertexes := make(chan []Vertex)

	go func() {
		defer close(errs)
		defer close(vertexes)

		data, ein := ReadNL(p, in, buffer)
		defer PushErrors(ein, errs)()

		for in := range data {
			q := make([]Vertex, len(in))

			for id := range in {
				q[id] = Vertex(in[id])
			}

			p.Job("READ_VERTEX").Add(len(q))
			vertexes <- q
		}
	}()

	return vertexes, errs
}

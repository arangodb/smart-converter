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
	"strings"
)

type EdgeVertex struct {
	From, To Vertex
}

type Vertex string

func ExtractEdgeVertexFromEdge(in io.Reader, out io.Writer, threads int) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		vertexes, vertexErr := ReadEdgesFromEdgeDocuments(in, threads)
		defer PushErrors(vertexErr, errs)()

		nout := make(chan []EdgeVertex)

		go func() {
			defer close(nout)
			for vertex := range vertexes {
				q := make([]EdgeVertex, len(vertex))

				RunInThread(threads, len(q), func(id int) {
					q[id].To = RemoveCollectionNameFromVertex(vertex[id].To)
					q[id].From = RemoveCollectionNameFromVertex(vertex[id].From)
				})

				nout <- q
			}
		}()

		defer PushErrors(WriteEdgeVertex(out, nout), errs)()
	}()
	return errs
}

func ExtractVertexFromDocuments(in io.Reader, out io.Writer, threads int) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		vertexes, vertexErr := ReadVertexFromVertexDocuments(in,  threads)
		defer PushErrors(vertexErr, errs)()

		nout := make(chan []Vertex)

		go func() {
			defer close(nout)
			for vertex := range vertexes {
				q := make([]Vertex, len(vertex))

				RunInThread(threads, len(q), func(id int) {
					q[id] = RemoveCollectionNameFromVertex(vertex[id])
				})

				nout <- q
			}
		}()

		defer PushErrors(WriteVertex(out, nout), errs)()
	}()
	return errs
}

func RemoveCollectionNameFromVertex(v Vertex) Vertex {
	z := strings.SplitN(string(v), "/", 2)

	if len(z) == 1 {
		return Vertex(z[0])
	}

	return Vertex(z[1])
}

func WriteEdgeVertex(writer io.Writer, docs <-chan []EdgeVertex) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		data := make(chan [][]byte)
		ein := WriteNL(writer, data)
		defer PushErrors(ein, errs)()

		for in := range docs {
			q := make([][]byte, len(in)*2)

			for id := range in {
				q[id*2] = []byte(in[id].From)
				q[id*2+1] = []byte(in[id].To)
			}

			data <- q
		}

		close(data)
	}()

	return errs
}

func WriteVertex(writer io.Writer, docs <-chan []Vertex) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		data := make(chan [][]byte)
		ein := WriteNL(writer, data)
		defer PushErrors(ein, errs)()

		for in := range docs {
			q := make([][]byte, len(in))

			for id := range in {
				q[id] = []byte(in[id])
			}

			data <- q
		}

		close(data)
	}()

	return errs
}

func ReadVertex(in io.Reader) (<-chan []Vertex, <-chan error) {
	errs := make(chan error, 32)
	vertexes := make(chan []Vertex)

	go func() {
		defer close(errs)
		defer close(vertexes)

		data, ein := ReadNL(in)
		defer PushErrors(ein, errs)()

		for in := range data {
			q := make([]Vertex, len(in))

			for id := range in {
				q[id] = Vertex(in[id])
			}

			vertexes <- q
		}
	}()

	return vertexes, errs
}

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
	"io"
	"os"
	"strings"
	"time"
)

type Vertex string

func (v Vertex) ID() string {
	z := strings.SplitN(string(v), "/", 2)

	if len(z) == 1 {
		return z[0]
	}

	return z[1]
}

func ExtractEdgeVertexFromEdge(handler Handler, in *os.File, out io.Writer, threads int) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		count := DiscoverL(handler, p, in.Name(), "edges")

		current := 0

		t := p.Task(time.Second, func(state string, duration time.Duration) {
			WithMemory(p.Info()).Msgf("%s (%s): Translating edges (%3.4f%%)", state, duration.String(), float64(100)*(float64(current)/float64(count)))
		})

		writter := bufio.NewWriterSize(out, MaxBufferSize)

		ReadEdge(handler, in, threads, func(sp Process, documents []EdgeDocument) {
			for id := range documents {
				if _, err := writter.WriteString(documents[id].From.ID()); err != nil {
					p.Emit(err)
				}
				if err := writter.WriteByte('\n'); err != nil {
					p.Emit(err)
				}
				if _, err := writter.WriteString(documents[id].To.ID()); err != nil {
					p.Emit(err)
				}
				if err := writter.WriteByte('\n'); err != nil {
					p.Emit(err)
				}
			}
			current += len(documents)
		}).Wait()

		if err := writter.Flush(); err != nil {
			p.Emit(err)
		}

		t.Done()
	}()

	return p
}

func ExtractVertexFromDocuments(handler Handler, in *os.File, out io.Writer, threads int) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		count := DiscoverL(handler, p, in.Name(), "vertexes")

		writter := bufio.NewWriterSize(out, MaxBufferSize)

		current := 0

		t := p.Task(time.Second, func(state string, duration time.Duration) {
			WithMemory(p.Info()).Msgf("%s (%s): Translating vertexes (%3.4f%%)", state, duration.String(), float64(100)*(float64(current)/float64(count)))
		})

		ReadDocument(handler, in, threads, func(sp Process, documents []VertexDocument) {
			for id := range documents {
				if _, err := writter.WriteString(documents[id].ID.ID()); err != nil {
					p.Emit(err)
				}
				if err := writter.WriteByte('\n'); err != nil {
					p.Emit(err)
				}
			}
			current += len(documents)
		}).Wait()

		if err := writter.Flush(); err != nil {
			p.Emit(err)
		}

		t.Done()
	}()

	return p
}

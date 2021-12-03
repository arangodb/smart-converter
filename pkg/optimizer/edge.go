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
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

type EdgeDocument struct {
	From Vertex `json:"_from"`
	To   Vertex `json:"_to"`
}

type EdgeReadHandler func(p Process, edges []EdgeDocument)

func ReadEdge(handler Handler, in io.Reader, threads int, h EdgeReadHandler) Process {
	p := handler.Process()

	go func() {
		defer p.Done()

		documents := make([]EdgeDocument, MaxBufferSize)

		ReadL(handler, in, func(sp Process, buffer []byte, parts [][]byte) {
			RunInThread(threads, len(parts), func(id int) {
				if err := json.Unmarshal(parts[id], &documents[id]); err != nil {
					p.Emit(errors.Wrapf(err, "Data (%d): %s", id, string(parts[id])))
				}
			})

			h(p, documents[:len(parts)])
		}).Wait()
	}()

	return p
}

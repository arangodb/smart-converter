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
)

type EdgeDocument struct {
	From Vertex `json:"_from"`
	To   Vertex `json:"_to"`
}

func ReadEdgesFromEdgeDocuments(reader io.Reader, threads int) (<-chan []EdgeVertex, <-chan error) {
	docs := make(chan []EdgeVertex)
	errs := make(chan error, 32)

	go func() {
		defer close(docs)
		defer close(errs)

		din, ein := ReadJSON(reader, threads, func(bytes []byte) (interface{}, error) {
			var v EdgeDocument

			if err := json.Unmarshal(bytes, &v); err != nil {
				return nil, err
			}

			return EdgeVertex{
				From: v.From,
				To:   v.To,
			}, nil
		})

		defer PushErrors(ein, errs)()

		for d := range din {
			q := make([]EdgeVertex, len(d))

			for id := range d {
				if d[id] == nil {
					continue
				}

				if v, ok := d[id].(EdgeVertex); ok {
					q[id] = v
				}
			}

			docs <- q
		}
	}()

	return docs, errs
}

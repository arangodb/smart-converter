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
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func ensureTestCase(t *testing.T, id string, docCount, edgeCount int, docCollection, edgeCollection string) {
	t.Run(fmt.Sprintf("Ensure test case %s", id), func(t *testing.T) {
		t.Parallel()

		docFile := fmt.Sprintf("case.%s.documents.in", id)
		edgeFile := fmt.Sprintf("case.%s.edges.in", id)

		if _, err := os.Stat(docFile); err == nil {
			t.Skip()
		}

		if _, err := os.Stat(edgeFile); err == nil {
			t.Skip()
		}

		docHandler, err := os.OpenFile(docFile, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
		require.NoError(t, err)

		edgeHandler, err := os.OpenFile(edgeFile, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
		require.NoError(t, err)

		generateTestCase(t, docHandler, edgeHandler, docCount, edgeCount, docCollection, edgeCollection)

		require.NoError(t, edgeHandler.Close())
		require.NoError(t, docHandler.Close())
	})
}

func generateTestCase(t *testing.T, outDocuments, outEdges io.Writer, docCount, edgeCount int, docCollection, edgeCollection string) {
	d := json.NewEncoder(outDocuments)
	e := json.NewEncoder(outEdges)

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < docCount; i++ {
			doc := map[string]interface{}{}

			doc["_key"] = fmt.Sprintf("%015d", i)
			doc["_id"] = fmt.Sprintf("%s/%s", docCollection, doc["_key"])

			require.NoError(t, d.Encode(doc))
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < edgeCount; {
			a, b := rand.Intn(docCount), rand.Intn(docCount)
			if a == b {
				continue
			}

			doc := map[string]interface{}{}

			doc["_key"] = fmt.Sprintf("%015d", i)
			doc["_id"] = fmt.Sprintf("%s/%s", edgeCollection, doc["_key"])
			doc["_from"] = fmt.Sprintf("%s/%s", docCollection, fmt.Sprintf("%015d", a))
			doc["_to"] = fmt.Sprintf("%s/%s", docCollection, fmt.Sprintf("%015d", b))

			require.NoError(t, e.Encode(doc))

			i++
		}
	}()

	wg.Wait()
}

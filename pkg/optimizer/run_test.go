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
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Run(t *testing.T) {
	t.Run("Generate", func(t *testing.T) {
		ensureTestCase(t, "T1", 64, 128, "colA", "edgeA")
		ensureTestCase(t, "T2", 1024*128, 1024*128*2, "colA", "edgeA")
		ensureTestCase(t, "T3", 1024*1024, 1024*1024*2, "colA", "edgeA")
		ensureTestCase(t, "T4", 16*1024*1024, 8*1024*1024, "colA", "edgeA")
	})

	t.Run("Execute", func(t *testing.T) {
		runExecution(t, "T1")
		runExecution(t, "T2")
		runExecution(t, "T3")
		runExecution(t, "T4")
	})
}

func runExecution(t *testing.T, id string) {
	t.Run(fmt.Sprintf("Case %s", id), func(t *testing.T) {
		docFile := fmt.Sprintf("case.%s.documents.in", id)
		edgeFile := fmt.Sprintf("case.%s.edges.in", id)

		vertexesFile := fmt.Sprintf("case.%s.vertexes.test", id)
		edgeVertexesFile := fmt.Sprintf("case.%s.edge-vertexes.test", id)
		edgeVertexesTempA := fmt.Sprintf("case.%s.edge-vertexes.parsed.A", id)
		edgeVertexesTempB := fmt.Sprintf("case.%s.edge-vertexes.parsed.B", id)
		edgeIntOut := fmt.Sprintf("case.%s.edge-vertexes.int", id)
		edgeIntOpt := fmt.Sprintf("case.%s.edge-vertexes.opt", id)

		docFileOut := fmt.Sprintf("case.%s.documents.out", id)
		edgeFileOut := fmt.Sprintf("case.%s.edges.out", id)

		t.Run("TESTING", func(t *testing.T) {
			t.Run("Extract vertexes", func(t *testing.T) {
				executeCommand(t, "extract-vertexes", "--in", docFile, "--out", vertexesFile)
			})

			t.Run("Extract edges", func(t *testing.T) {
				executeCommand(t, "extract-edges", "--in", edgeFile, "--out", edgeVertexesFile)
			})

			t.Run("Map edges", func(t *testing.T) {
				executeCommand(t, "map-edges", "--vertexes", vertexesFile, "--edges", edgeVertexesFile, "--temp.a", edgeVertexesTempA, "--temp.b", edgeVertexesTempB, "--out", edgeIntOut)
			})

			t.Run("Optimize", func(t *testing.T) {
				executeCommand(t, "optimize", "--in", edgeIntOut, "--out", edgeIntOpt)
			})

			t.Run("Translate", func(t *testing.T) {
				t.Run("Vertexes", func(t *testing.T) {
					executeCommand(t, "translate", "vertex", "--map", edgeIntOpt, "--vertexes", docFile, "--vertexes-out", docFileOut)
				})
				t.Run("Edges", func(t *testing.T) {
					executeCommand(t, "translate", "edge", "--map", edgeIntOpt, "--vertexes", docFile, "--edges", edgeFile, "--edge-map", edgeIntOut, "--edges-out", edgeFileOut)
				})
			})
		})
	})
}

func executeCommand(t *testing.T, args ...string) {
	cmd := CLI()

	cmd.SetArgs(args)

	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	m := GetMemStats()

	defer func() {
		n := GetMemStats()
		t.Logf("Allocated %dM", bToMb(n.TotalAlloc-m.TotalAlloc))
	}()

	require.NoError(t, cmd.Execute())
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

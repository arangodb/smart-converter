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
	"bytes"
	"testing"

	"github.com/rs/zerolog/log"
)

func Test_Delimit(t *testing.T) {
	t.Run("With keyword", func(t *testing.T) {
		in := []byte("BEST\nZZZ\nu\n")
		b := bytes.NewBuffer(in)

		h := NewHandler(log.Logger)

		ReadSizeDelimit(h, b, '\n', func(p Process, buffer []byte, parts [][]byte) {
			for _, p := range parts {
				println(string(p))
			}
		})

		h.Wait()
	})
}

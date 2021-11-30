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
	"runtime"

	"github.com/rs/zerolog"
)

func GetMemStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func WithMemory(event *zerolog.Event) *zerolog.Event {
	return event.Str("memory", CurrentMemory())
}

func CurrentMemory() string {
	return MemoryToString(GetMemStats().Sys)
}

func MemoryToString(in uint64) string {
	return memoryToStringS(float64(in))
}

func memoryToStringS(in float64) string {
	v, p := memoryToString(in, 0)

	post := ""

	switch p {
	case 1:
		post = "KiB"
	case 2:
		post = "MiB"
	case 3:
		post = "GiB"
	default:
		post = "NA"
	}

	return fmt.Sprintf("%.2f%s", v, post)
}

func memoryToString(in float64, prefix int) (float64, int) {
	if prefix == 3 {
		return in, prefix
	}

	if in > 1024 {
		return memoryToString(in/1024, prefix+1)
	}

	return in, prefix
}

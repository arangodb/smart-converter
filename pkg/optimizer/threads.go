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

import "sync"

func ThreadChannel(size int) <-chan int {
	c := make(chan int, size)

	for i := 0; i < size; i++ {
		c <- i
	}

	close(c)

	return c
}

func RunInThread(threads, size int, f func(id int)) {
	var wg sync.WaitGroup

	c := ThreadChannel(size)

	for i := 0; i < threads; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for id := range c {
				f(id)
			}
		}()
	}

	wg.Wait()
}

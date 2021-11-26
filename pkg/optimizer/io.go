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
	"errors"
	"io"
	"sync"

	"k8s.io/apimachinery/pkg/util/json"
)

func ThreadChannel(size int) <-chan int {
	c := make(chan int, size)

	for i := 0; i < size; i++ {
		c <- i
	}

	close(c)

	return c
}

func Run(threads int, f func()) {
	var wg sync.WaitGroup

	for i := 0; i < threads; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			f()
		}()
	}

	wg.Wait()
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

func ReadJSONAny(p Progress, in io.Reader, buffer, threads int) (<-chan []interface{}, <-chan error) {
	return ReadJSON(p, in, buffer, threads, func(bytes []byte) (interface{}, error) {
		var i interface{}

		if err := json.Unmarshal(bytes, &i); err != nil {
			return nil, err
		}

		return i, nil
	})
}

func ReadJSON(p Progress, in io.Reader, buffer, threads int, parser func([]byte) (interface{}, error)) (<-chan []interface{}, <-chan error) {
	errs := make(chan error, 32)
	data := make(chan []interface{})

	go func() {
		defer close(errs)
		defer close(data)

		din, ein := ReadNL(p, in, buffer)
		defer PushErrors(ein, errs)()

		for arr := range din {
			out := make([]interface{}, len(arr))

			RunInThread(threads, len(arr), func(id int) {
				v, err := parser(arr[id])
				if err != nil {
					errs <- err
					return
				}
				out[id] = v
			})

			p.Job("JSON_READ").Add(len(out))
			data <- out
		}
	}()

	return data, errs
}

func ReadNL(p Progress, in io.Reader, buffer int) (<-chan [][]byte, <-chan error) {
	errs := make(chan error, 32)
	data := make(chan [][]byte)

	go func() {
		defer close(errs)
		defer close(data)

		scanner := bufio.NewReaderSize(in, 4*1024*1024)

		datas := make([][]byte, buffer)
		size := 0

		for {
			line, err := scanner.ReadSlice('\n')
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				errs <- err
				continue
			}
			l := line[0 : len(line)-1]
			q := make([]byte, len(l))
			copy(q, l)
			datas[size] = q
			size++

			if size == buffer {
				p.Job("READ").Add(size)
				data <- datas

				datas = make([][]byte, buffer)
				size = 0
			}
		}

		if size > 0 {
			p.Job("READ").Add(size)
			data <- datas[0:size]
		}
	}()

	return data, errs
}

func WriteJSON(p Progress, out io.Writer, data <-chan []interface{}, threads int) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		dataOut := make(chan [][]byte, 4)
		ein := WriteNL(p, out, dataOut)
		defer PushErrors(ein, errs)()

		for arr := range data {
			out := make([][]byte, len(arr))
			RunInThread(threads, len(arr), func(id int) {
				v, err := json.Marshal(arr[id])
				if err != nil {
					errs <- err
					return
				}

				out[id] = v
			})
			p.Job("WROTE_JSON").Add(len(out))
			dataOut <- out
		}

		close(dataOut)
	}()

	return errs
}

func WriteNL(p Progress, out io.Writer, data <-chan [][]byte) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		scanner := bufio.NewWriterSize(out, 4*1024*1024)

		for in := range data {
			for id := range in {
				if _, err := scanner.Write(in[id]); err != nil {
					errs <- err

				}
				if _, err := scanner.WriteString("\n"); err != nil {
					errs <- err

				}
			}

			p.Job("WRITE_NL").Add(len(in))
		}

		if err := scanner.Flush(); err != nil {
			errs <- err
		}
	}()

	return errs
}

func Write(p Progress, out io.Writer, data <-chan [][]byte) <-chan error {
	errs := make(chan error, 32)

	go func() {
		defer close(errs)

		scanner := bufio.NewWriterSize(out, 4*1024*1024)

		for in := range data {
			for id := range in {
				if _, err := scanner.Write(in[id]); err != nil {
					errs <- err

				}
			}

			p.Job("WRITE").Add(len(in))
		}

		if err := scanner.Flush(); err != nil {
			errs <- err
		}
	}()

	return errs
}

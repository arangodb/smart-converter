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
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

func NewHandler(logger zerolog.Logger) Handler {
	return &handler{
		errors: make(chan error, 32),
		logger: logger,
	}
}

type Handler interface {
	Process() ProcessInt

	Errors() <-chan error

	Wait()
}

type handler struct {
	errors chan error

	wg sync.WaitGroup

	logger zerolog.Logger
}

func (h *handler) Process() ProcessInt {
	h.wg.Add(1)
	p := &process{
		handler: h,
		closeCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
	}

	go p.run()

	return p
}

func (h *handler) Errors() <-chan error {
	return h.errors
}

func (h *handler) Wait() {
	defer close(h.errors)
	h.wg.Wait()
}

type ErrorEmitFunc func() error

type Process interface {
	Wait()
}

type ProcessInt interface {
	Process

	Emit(err ...error)
	DeferEmit(e ...ErrorEmitFunc)

	Info() *zerolog.Event

	Task(interval time.Duration, exec func(state string, duration time.Duration)) Task

	Done()
}

type process struct {
	lock sync.Mutex

	closeCh chan struct{}
	doneCh  chan struct{}

	handler *handler
}

func (p *process) Task(interval time.Duration, exec func(state string, duration time.Duration)) Task {
	t := &task{
		interval: interval,
		exec:     exec,
		done:     make(chan struct{}),
		closed:   make(chan struct{}),
	}

	go t.run()

	return t
}

func (p *process) Info() *zerolog.Event {
	return p.handler.logger.Info()
}

func (p *process) DeferEmit(e ...ErrorEmitFunc) {
	for _, q := range e {
		if err := q(); err != nil {
			p.Emit(err)
		}
	}
}

func (p *process) Wait() {
	<-p.doneCh
}

func (p *process) Emit(err ...error) {
	for _, e := range err {
		p.handler.errors <- e
	}
}

func (p *process) Done() {
	p.lock.Lock()
	defer p.lock.Unlock()

	select {
	case <-p.closeCh:
		return
	default:
		close(p.closeCh)
	}
}

func (p *process) run() {
	defer p.handler.wg.Done()
	defer close(p.doneCh)

	<-p.closeCh
}

type Task interface {
	Done()
}

type task struct {
	lock sync.Mutex

	interval time.Duration

	exec func(state string, duration time.Duration)

	done, closed chan struct{}
}

func (t *task) Done() {
	t.lock.Lock()
	defer t.lock.Unlock()

	select {
	case <-t.done:
		return
	default:
		close(t.done)
	}

	<-t.closed
}

func (t *task) run() {
	defer close(t.closed)

	i := time.NewTicker(t.interval)
	defer i.Stop()

	n := time.Now()

	defer func() {
		t.exec("DONE", time.Since(n))
	}()
	t.exec("STARTING", time.Since(n))

	for {
		select {
		case <-i.C:
			t.exec("PROGRESS", time.Since(n))
		case <-t.done:
			return
		}
	}
}

func DiscoverL(h Handler, p ProcessInt, file, name string) int {
	r, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		p.Emit(err)
		return 0
	}

	defer p.DeferEmit(r.Close)

	current := 0

	defer p.Task(5*time.Second, func(state string, duration time.Duration) {
		WithMemory(p.Info()).Msgf("%s (%s): Discovering count of %s (discovered %d)", state, duration.String(), name, current)
	}).Done()

	ReadL(h, r, func(p Process, buffer []byte, parts [][]byte) {
		current += len(parts)
	}).Wait()

	runtime.GC()

	return current
}

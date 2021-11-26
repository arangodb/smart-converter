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
	"sync"
	"time"

	"github.com/rs/zerolog"
)

func WithName(f func() *zerolog.Event) EventMaker {
	return func(name string) *zerolog.Event {
		return f().Str("name", name)
	}
}

type EventMaker func(name string) *zerolog.Event

func NewProgress(interval time.Duration, ev EventMaker) Progress {
	n := time.Now()
	p := &progress{
		start: n,
		last:  n,
		close: make(chan struct{}),
		done:  make(chan struct{}),
		ev:    ev,
		jobs:  map[string]*jobProgress{},
	}

	go p.do(interval)

	return p
}

type Progress interface {
	Job(name string) JobProgress

	Done()
}

type JobProgress interface {
	Inc()
	Add(value int)
}

type progress struct {
	lock sync.Mutex

	start, last time.Time

	close, done chan struct{}

	ev EventMaker

	jobs map[string]*jobProgress
}

type jobProgress struct {
	lock sync.Mutex

	progress *progress

	sum   int
	count int
}

func (p *progress) Done() {
	p.markDone()

	<-p.done
}

func (p *progress) Job(name string) JobProgress {
	p.lock.Lock()
	defer p.lock.Unlock()

	if v, ok := p.jobs[name]; ok {
		return v
	}

	p.jobs[name] = &jobProgress{
		progress: p,
	}

	return p.jobs[name]
}

func (p *progress) markDone() {
	p.lock.Lock()
	defer p.lock.Unlock()

	select {
	case <-p.close:
		return
	default:
		close(p.close)
	}
}

func (p *progress) do(interval time.Duration) {
	defer close(p.done)

	tickerT := time.NewTicker(interval)
	defer tickerT.Stop()

	for {
		select {
		case <-tickerT.C:
			p.printProgress()
		case <-p.close:
			p.printDone()
			return
		}
	}
}

func (p *progress) printProgress() {
	p.lock.Lock()
	defer p.lock.Unlock()

	since := time.Since(p.last)
	sinceStart := time.Since(p.start)

	for n, j := range p.jobs {
		c, s := j.get()
		avgIter := int(float64(c) / (float64(since) / float64(time.Second)))
		startIter := int(float64(s) / (float64(sinceStart) / float64(time.Second)))

		p.ev(n).Msgf("Progress: Executed %d (total %d), iteration %s (total %s), with avg %d/s (total %d/s)", c, s, since.String(), sinceStart.String(), avgIter, startIter)
	}

	p.last = time.Now()
}

func (p *progress) printDone() {
	p.lock.Lock()
	defer p.lock.Unlock()
	sinceStart := time.Since(p.start)

	for n, j := range p.jobs {
		_, s := j.get()
		startIter := int(float64(s) / (float64(sinceStart) / float64(time.Second)))
		p.ev(n).Msgf("Done in %s, Total %d, AVG %d/s", time.Since(p.start).String(), s, startIter)
	}
}

func (j *jobProgress) Inc() {
	j.Add(1)
}

func (j *jobProgress) Add(value int) {
	j.lock.Lock()
	defer j.lock.Unlock()

	j.count += value
	j.sum += value
}

func (j *jobProgress) get() (int, int) {
	j.lock.Lock()
	defer j.lock.Unlock()

	c := j.count
	j.count = 0

	return c, j.sum
}

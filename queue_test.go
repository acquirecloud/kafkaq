// Copyright 2023 The acquirecloud Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package kafkaq

import (
	"context"
	"fmt"
	"github.com/acquirecloud/golibs/kvs/inmem"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func BenchmarkGetChan(b *testing.B) {
	p := newCPool()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := p.Get().(chan Task)
		p.Put(c)
	}
}

func Test_queue_readwrite(t *testing.T) {
	q, p := testQueue()
	defer p.Shutdown()
	defer q.Shutdown()

	task := Task("test")
	assert.Nil(t, p.Publish(task))
	j, err := q.GetJob(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, task, j.Task())

	assert.Nil(t, j.Done())
	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.Timeout*2)
	defer cancel()
	_, err = q.GetJob(ctx)
	assert.Equal(t, ctx.Err(), err)
}

func Test_queue_timeout(t *testing.T) {
	q, p := testQueue()
	defer p.Shutdown()
	defer q.Shutdown()

	task := Task("test")
	assert.Nil(t, p.Publish(task))
	start := time.Now()
	j, err := q.GetJob(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, task, j.Task())

	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.Timeout*2)
	defer cancel()
	j, err = q.GetJob(ctx)
	assert.Nil(t, err)
	assert.Equal(t, task, j.Task())
	assert.True(t, time.Now().Add(q.cfg.Timeout).After(start))

	assert.Nil(t, j.Done())

	ctx, cancel1 := context.WithTimeout(context.Background(), q.cfg.Timeout*2)
	defer cancel1()
	_, err = q.GetJob(ctx)
	assert.Equal(t, ctx.Err(), err)
}

func Test_queue_setPos(t *testing.T) {
	q, p := testQueue()
	defer p.Shutdown()
	defer q.Shutdown()

	task := Task("test")
	assert.Nil(t, p.Publish(task))
	j, err := q.GetJob(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, task, j.Task())

	q.kfClient.(*imclient).reset()

	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.Timeout/2)
	defer cancel()
	_, err = q.GetJob(ctx)
	assert.Equal(t, ctx.Err(), err)
}

func Test_queue_batch(t *testing.T) {
	q, p := testQueue()
	defer p.Shutdown()
	defer q.Shutdown()

	task := Task("test")
	assert.Nil(t, p.Publish(task))
	j, err := q.GetJob(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, task, j.Task())

	q.kfClient.(*imclient).reset()

	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.Timeout/2)
	defer cancel()
	_, err = q.GetJob(ctx)
	assert.Equal(t, ctx.Err(), err)
}

func Test_queue_mix(t *testing.T) {
	q, p := testQueue()
	defer p.Shutdown()
	defer q.Shutdown()

	for i := 0; i < 1000; i++ {
		assert.Nil(t, q.Publish(Task(fmt.Sprintf("%d", i))))
	}

	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.Timeout+q.cfg.Timeout/2)
	defer cancel()

	cnt := 0
	for i := 0; i < 1000; i++ {
		j, err := q.GetJob(ctx)
		assert.Nil(t, err)
		assert.Equal(t, j.Task(), Task(fmt.Sprintf("%d", i)))
		if i < 500 {
			j.Done()
			cnt++
		}
	}

	for ctx.Err() == nil {
		j, err := q.GetJob(ctx)
		if err != nil {
			break
		}
		assert.Nil(t, j.Done())
		cnt++
		if cnt <= 1000 {
			assert.Nil(t, p.Publish(Task(fmt.Sprintf("%d", cnt+1000))))
		}
	}

	assert.Equal(t, 1500, cnt)
}

func testQueue() (*queue, *publisher) {
	kvs := inmem.New()
	krw := newIMClient()
	cfg := GetDefaultConfig()
	cfg.Timeout = 250 * time.Millisecond
	q := NewQueue(kvs, krw, cfg)
	q.jobExpMs = 100
	q.Init(nil)
	p := NewPublisher(krw, cfg.Topic)
	return q, p
}

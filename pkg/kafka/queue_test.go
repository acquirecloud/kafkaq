// Copyright 2023 The acquirecloud Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package kafka

import (
	"context"
	"fmt"
	"github.com/acquirecloud/kafkaq"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func BenchmarkGetChan(b *testing.B) {
	p := newCPool()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := p.Get().(chan kafkaq.Task)
		p.Put(c)
	}
}

func Test_queue_readwrite(t *testing.T) {
	q, p := testQueue()
	defer p.Shutdown()
	defer q.Shutdown()

	task := kafkaq.Task("test")
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

	task := kafkaq.Task("test")
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

	task := kafkaq.Task("test")
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

	task := kafkaq.Task("test")
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
		assert.Nil(t, q.Publish(kafkaq.Task(fmt.Sprintf("%d", i))))
	}

	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.Timeout+q.cfg.Timeout/2)
	defer cancel()

	cnt := 0
	for i := 0; i < 1000; i++ {
		j, err := q.GetJob(ctx)
		assert.Nil(t, err)
		assert.Equal(t, j.Task(), kafkaq.Task(fmt.Sprintf("%d", i)))
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
			assert.Nil(t, p.Publish(kafkaq.Task(fmt.Sprintf("%d", cnt+1000))))
		}
	}

	assert.Equal(t, 1500, cnt)
}

func testQueue() (*queue, *publisher) {
	cfg := GetDefaultQueueConfig()
	cfg.Timeout = 250 * time.Millisecond
	q := NewInMem(cfg)
	q.jobExpMs = 100
	q.Init(nil)
	p := newPublisher(q.kfClient, cfg.Topic)
	return q, p
}

// run it with real envs (uncomment if needed)
func __TestNewKafkaRedis(t *testing.T) {
	q := NewKafkaRedis(GetDefaultQueueConfig(),
		// docker options
		&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	assert.Nil(t, q.Init(nil))
	defer q.Shutdown()
	p := NewPublisher(GetDefaultQueueConfig())
	defer p.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan bool)
	done := make(chan bool)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(wID int) {
			defer fmt.Println("closing worker ", wID)
			defer wg.Done()
			for ctx.Err() == nil {
				fmt.Println(" worker before getJob ", wID)
				j, err := q.GetJob(ctx)
				fmt.Println(" worker after getJob ", wID)

				if err == nil {
					fmt.Println("worker ", wID, ": executing ", string(j.Task()))
					j.Done()
					select {
					case c <- true:
					case <-done:
						return
					}
				}
			}
		}(i)
	}

	total := 100

	for j := 0; j < 10; j++ {
		start := time.Now()
		for i := 0; i < total; i++ {
			assert.Nil(t, p.Publish(kafkaq.Task(fmt.Sprintf("task %d-%d", j, i))))
		}
		count := 0
		for count < total {
			select {
			case <-c:
				count++
			}
		}
		diff := time.Now().Sub(start)
		fmt.Println("total ", diff, " ", diff/time.Duration(total), " per one request")
	}
	cancel()
	close(done)
	wg.Wait()
}

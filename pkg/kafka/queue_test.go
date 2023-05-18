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
	"github.com/acquirecloud/golibs/logging"
	"github.com/acquirecloud/kafkaq"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
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
		c := p.Get().(jobCh)
		_ = c
		//p.Put(c)
	}
}

func Test_queue_readwrite(t *testing.T) {
	q, _ := testQueue()
	defer q.Shutdown()

	task := kafkaq.Task("test")
	_, err := q.Publish(task)
	assert.Nil(t, err)
	j, err := q.GetJob(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, task, j.Task())

	assert.Nil(t, j.Done())
	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.Timeout*2)
	defer cancel()
	_, err = q.GetJob(ctx)
	assert.Equal(t, ctx.Err(), err)
}

func Test_queue_readwrite2(t *testing.T) {
	q, p := testQueue()
	defer q.Shutdown()
	defer p.Shutdown()

	task := kafkaq.Task("test")
	_, err := p.Publish(task)
	assert.Nil(t, err)
	// for the publish only queue the GetJob should return an error
	_, err = p.GetJob(context.Background())
	assert.NotNil(t, err)

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
	q, _ := testQueue()
	defer q.Shutdown()

	task := kafkaq.Task("test")
	_, err := q.Publish(task)
	assert.Nil(t, err)
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
	q, _ := testQueue()
	defer q.Shutdown()

	task := kafkaq.Task("test")
	_, err := q.Publish(task)
	assert.Nil(t, err)
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
	q, _ := testQueue()
	defer q.Shutdown()

	task := kafkaq.Task("test")
	_, err := q.Publish(task)
	assert.Nil(t, err)
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
	q, _ := testQueue()
	defer q.Shutdown()

	for i := 0; i < 1000; i++ {
		_, err := q.Publish(kafkaq.Task(fmt.Sprintf("%d", i)))
		assert.Nil(t, err)
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
			_, err := q.Publish(kafkaq.Task(fmt.Sprintf("%d", cnt+1000)))
			assert.Nil(t, err)
		}
	}

	assert.Equal(t, 1500, cnt)
}

func Test_kRec(t *testing.T) {
	var kr kRec
	assert.Equal(t, int64(0), kr.nextRunAtMillis())
	assert.Nil(t, kr.verBuf())

	var ver uuid.UUID
	kr = ver[:]
	assert.Equal(t, ver, kr.ver())
}

func Test_JobInfo(t *testing.T) {
	q, _ := testQueue()
	q.cfg.Timeout = time.Millisecond * 100
	defer q.Shutdown()

	ji, err := q.Get("lala")
	assert.Nil(t, err)
	assert.Equal(t, kafkaq.JobInfo{ID: "lala", Status: kafkaq.JobStatusUnknown}, ji)

	id, err := q.Publish(kafkaq.Task("lalaa"))
	assert.Nil(t, err)
	ji, err = q.Get(id)
	assert.Nil(t, err)
	assert.Equal(t, kafkaq.JobInfo{ID: id, Status: kafkaq.JobStatusUnknown}, ji)

	start := time.Now()
	_, err = q.GetJob(context.Background())
	assert.Nil(t, err)
	ji, err = q.Get(id)
	assert.Nil(t, err)
	assert.Equal(t, kafkaq.JobInfo{ID: id, Status: kafkaq.JobStatusProcessing, NextExecutionAt: ji.NextExecutionAt}, ji)
	assert.True(t, ji.NextExecutionAt.After(start))
	assert.True(t, ji.NextExecutionAt.Before(time.Now().Add(time.Millisecond*200)))

	time.Sleep(150)
	j, err := q.GetJob(context.Background())
	ji1, err := q.Get(j.ID())
	assert.Nil(t, err)
	ji.Rescedules = 1
	ji.NextExecutionAt = ji1.NextExecutionAt
	assert.Equal(t, ji, ji1)

	j.Done()
	ji1, _ = q.Get(j.ID())
	var tm time.Time
	ji.NextExecutionAt = tm
	ji.Status = kafkaq.JobStatusDone
	assert.Equal(t, ji, ji1)
	ji1, err = q.Get(ji.ID)
	assert.Nil(t, err)
	assert.Equal(t, ji, ji1)

	id, err = q.Publish(kafkaq.Task("lalaa"))
	assert.Nil(t, err)
	_, err = q.GetJob(context.Background())
	assert.Nil(t, err)

	ji, err = q.Cancel(id)
	assert.Nil(t, err)
	assert.Equal(t, kafkaq.JobInfo{ID: id, Status: kafkaq.JobStatusDone}, ji)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*150)
	defer cancel()
	_, err = q.GetJob(ctx)
	assert.Equal(t, ctx.Err(), err)
}

func testQueue() (*queue, *queue) {
	cfg := GetDefaultQueueConfig()
	cfg.Timeout = 250 * time.Millisecond
	q := NewInMem(cfg)
	q.jobExpMs = 100
	q.Init(nil)
	cfg.PublishOnly = true
	q1 := NewInMem(cfg)
	q1.kvs = q.kvs
	q1.kfClient = q.kfClient
	return q, q1
}

// run it with real envs (uncomment if needed)
func __TestNewKafkaRedis(t *testing.T) {
	//q := NewKafkaKVS(GetDefaultQueueConfig(), inmem.New())
	q := NewKafkaRedis(GetDefaultQueueConfig(),
		// docker options
		&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	assert.Nil(t, q.Init(nil))
	defer q.Shutdown()

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
			_, err := q.Publish(kafkaq.Task(fmt.Sprintf("task %d-%d", j, i)))
			assert.Nil(t, err)
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

func __TestConsumeEndless(t *testing.T) {
	logging.SetLevel(logging.TRACE)
	cfg := GetDefaultQueueConfig()
	q := NewKafkaRedis(cfg,
		// docker options
		&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	assert.Nil(t, q.Init(nil))
	defer q.Shutdown()

	for {
		j, err := q.GetJob(context.Background())
		assert.Nil(t, err)
		j.Done()
		fmt.Println(j)
	}
}

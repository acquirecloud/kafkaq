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
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

// run it with real envs
func __TestNewKafkaRedis(t *testing.T) {
	q := NewKafkaRedis(KClientConfig{Brokers: []string{"localhost:9092"}, GroupID: "test"},
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
			assert.Nil(t, q.Publish(Task(fmt.Sprintf("task %d-%d", j, i))))
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

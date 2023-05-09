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

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			for ctx.Err() == nil {
				j, err := q.GetJob(ctx)
				if err == nil {
					fmt.Println("worker ", wID, ": executing ", string(j.Task()))
					j.Done()
					c <- true
				}
			}
		}(i)
	}

	start := time.Now()
	total := 10

	for i := 0; i < total; i++ {
		q.Publish(Task(fmt.Sprintf("task %d", i)))
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
	cancel()
	wg.Wait()
}

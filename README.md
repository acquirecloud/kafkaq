# Simple, reliable, low-latency task queue library in Golang
Kafkaq is a small Go library for queueing tasks and processing them asynchronously with workers. 

High-level overview of how Kafkaq works:
- A client publishes tasks in the queue (message broker like Kafka, Red Panda ...).
- A job executor (worker) polls the queue for tasks to be processed.
- Acquired tasks states stored in a key-value store to control their execution status. 
- Unconfirmed tasks are distributed again in the timeout. 

The system consists of the following components:
- The message broker which persists tasks (Kafka or similar tasks log-based persister)
- The tasks' state storage (Redis, Zookeeper, etcd...) to keep a track of the state of the tasks acquired for processing.
- The Go library for publishing and consuming tasks on the queue.

## Features
- Guaranteed at least one execution of each task
- Low latency (microseconds) (limited by Kafka or key-value storage like Redis/etcd which one is slower)
- Rescheduling uncommitted tasks after timeout
- Highly-reliable (limited by message broker or key-value storage reliabilities)
- Highly-scalable (each the system component maybe scaled horizontally)

## Quickstart

Make sure you have Go installed ([download](https://golang.org/dl/)). Version `1.19` or higher is recommended.

```sh
go get -u github.com/acquirecloud/kafkaq
```

Make sure you're running a Redis server locally or from a [Docker](https://hub.docker.com/_/redis) container.
Make sure you're running Kafka server locally or from a [Docker](https://hub.docker.com/_/redis) container.

NOTE: you can use docker compose, which runs Redis and Kafka in the docker, provided in [scripts](./scripts/run-in-docker) folder. 

Then you can try the following code, but don't be scared that it takes some time to start, this is due to default Kafka config:

```go
package main

import (
	"context"
	"fmt"
	"github.com/acquireclous/kafkaq"
	"github.com/go-redis/redis/v8"
	"sync"
	"time"
)

func main() {
	// creating the queue client. It allows to publish new tasks and receiving existing ones from the queue 
	q := kafkaq.NewKafkaRedis(kafkaq.KClientConfig{Brokers: []string{"localhost:9092"}, GroupID: "test"},
		// docker options
		&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	defer q.Shutdown() // q must be shutdown
	
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan bool)
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			for ctx.Err() == nil {
				// getting the job to process
				j, err := q.GetJob(ctx)
				if err == nil {
					fmt.Println("worker ", wID, ": executing ", string(j.Task()))
					j.Done() // commit it
					select {
					case c <- true:
                    case <- done:
                        return
					}
				}
			}
		}(i)
	}

	total := 10

	for j := 0; j < 10; j++ {
		start := time.Now()
		for i := 0; i < total; i++ {
			fmt.Println("publishing task ", i)
			// Publishing new task, each task is just "task X" string
			q.Publish(kafkaq.Task(fmt.Sprintf("task %d", i)))
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
```

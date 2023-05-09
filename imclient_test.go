package kafkaq

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func Test_imclient_read(t *testing.T) {
	c := newIMClient()
	assert.Nil(t, c.write(context.Background(), "la la", kafkaMessage{key: "1"}))
	km, err := c.read(context.Background(), "la la")
	assert.Nil(t, err)
	assert.Equal(t, "1", km.key)

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*100)
	_, err = c.read(ctx, "la la")
	assert.NotNil(t, err)
	assert.Equal(t, ctx.Err(), err)

	c.reset()
	assert.Nil(t, c.write(context.Background(), "la la", kafkaMessage{key: "1"}))
	km, err = c.read(context.Background(), "la la")
	assert.Nil(t, err)
	assert.Equal(t, "1", km.key)
}

func Test_imclient_commit(t *testing.T) {
	c := newIMClient()
	assert.Nil(t, c.write(context.Background(), "la la", kafkaMessage{key: "1"}))
	assert.Nil(t, c.write(context.Background(), "la la", kafkaMessage{key: "2"}))
	assert.Nil(t, c.write(context.Background(), "la la", kafkaMessage{key: "3"}))

	km, err := c.read(context.Background(), "la la")
	assert.Nil(t, err)
	assert.Equal(t, "1", km.key)

	km, err = c.read(context.Background(), "la la")
	assert.Nil(t, err)
	assert.Equal(t, "2", km.key)
	c.commit(context.Background(), km)

	km, err = c.read(context.Background(), "la la")
	assert.Nil(t, err)
	assert.Equal(t, "3", km.key)

	c.reset()
	km, err = c.read(context.Background(), "la la")
	assert.Nil(t, err)
	assert.Equal(t, "3", km.key)
}

func Test_multiread(t *testing.T) {
	var wg sync.WaitGroup
	var count int64
	c := newIMClient()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				km, err := c.read(ctx, "topic")
				if err != nil {
					return
				}
				i, _ := strconv.ParseInt(km.key, 10, 64)
				atomic.AddInt64(&count, i)
			}
		}()
	}

	for i := 0; i < 10000; i++ {
		assert.Nil(t, c.write(ctx, "topic", kafkaMessage{key: fmt.Sprintf("%d", i)}))
	}
	total := int64(9999 * 5000)
	for atomic.LoadInt64(&count) != total {
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()
}

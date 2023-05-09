package kafkaq

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// run it with real envs
func __TestKClient(t *testing.T) {
	kc := newKClient(KClientConfig{Brokers: []string{"localhost:9092"}, GroupID: "test"})
	defer kc.Close()
	start := time.Now()
	count := 10000
	for i := 0; i < count; i++ {
		err := kc.write(context.Background(), "kqMain", kafkaMessage{key: "abc", task: []byte("def")})
		assert.Nil(t, err)
	}
	for i := 0; i < count; i++ {
		msg, err := kc.read(context.Background(), "kqMain")
		assert.Nil(t, err)
		kc.commit(context.Background(), msg)
	}
	diff := time.Now().Sub(start)
	fmt.Println("end ", diff/time.Duration(count), " per request, total ", diff)
}

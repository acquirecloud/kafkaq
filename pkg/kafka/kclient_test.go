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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// run it with real envs
func __TestKClient(t *testing.T) {
	kc := newKClient(kClientConfig{brokers: []string{"localhost:9092"}, groupID: "test"})
	defer kc.Close()
	start := time.Now()
	count := 10
	for i := 0; i < count; i++ {
		err := kc.write(context.Background(), "kqMain", kafkaMessage{key: "abc", task: []byte("def")})
		fmt.Println("la la la", i)
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

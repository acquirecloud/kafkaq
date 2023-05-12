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
	"github.com/acquirecloud/golibs/container/iterable"
	"github.com/acquirecloud/golibs/errors"
	"github.com/google/uuid"
	"sync"
)

type (
	// imclient is the in-mem client which implements kafkaReadWriter for a consumer Group
	imclient struct {
		topics map[string]*imTopic
		lock   sync.Mutex
	}

	// imTopic struct represents the in-memory topic
	imTopic struct {
		lock sync.Mutex
		buf  *iterable.Map[string, *kafkaMessage]
		it   iterable.Iterator[iterable.MapEntry[string, *kafkaMessage]]
		c    chan struct{}
	}

	// imRef is the reference object used for committing messages consumed from the imclient
	imRef struct {
		topic string
		key   string
	}
)

var _ kafkaReadWriter = (*imclient)(nil)

// newIMClient returns the new imclient instance
func newIMClient() *imclient {
	imc := new(imclient)
	imc.topics = make(map[string]*imTopic)
	return imc
}

func (imc *imclient) read(ctx context.Context, topic string) (kafkaMessage, error) {
	imc.lock.Lock()
	imt, ok := imc.topics[topic]
	if !ok {
		imt = newIMTopic()
		imc.topics[topic] = imt
	}
	imc.lock.Unlock()
	return imt.read(ctx)
}

func (imc *imclient) commit(ctx context.Context, km kafkaMessage) error {
	imc.lock.Lock()
	defer imc.lock.Unlock()
	r := km.ref.(imRef)
	imt, ok := imc.topics[r.topic]
	if !ok {
		return errors.ErrNotExist
	}
	imt.commit(r.key)
	return nil
}

func (imc *imclient) write(ctx context.Context, topic string, km kafkaMessage) error {
	imc.lock.Lock()
	defer imc.lock.Unlock()
	imt, ok := imc.topics[topic]
	if !ok {
		imt = newIMTopic()
		imc.topics[topic] = imt
	}
	return imt.write(topic, km)
}

// reset allows to start reading from the top of the buffer. Some already messages in the
// fly may be processed again
func (imc *imclient) reset() {
	imc.lock.Lock()
	defer imc.lock.Unlock()

	for _, imt := range imc.topics {
		imt.reset()
	}
}

func newIMTopic() *imTopic {
	res := &imTopic{buf: iterable.NewMap[string, *kafkaMessage]()}
	res.it = res.buf.Iterator()
	return res
}

func (imt *imTopic) read(ctx context.Context) (kafkaMessage, error) {
	for {
		imt.lock.Lock()
		if imt.it.HasNext() {
			e, _ := imt.it.Next()
			defer imt.lock.Unlock()
			return *e.Value, nil
		}
		if imt.c == nil {
			imt.c = make(chan struct{})
		}
		c := imt.c
		imt.lock.Unlock()
		select {
		case <-c:
		case <-ctx.Done():
			return kafkaMessage{}, ctx.Err()
		}
	}
}

func (imt *imTopic) write(topic string, km kafkaMessage) error {
	imt.lock.Lock()
	defer imt.lock.Unlock()
	ref := imRef{key: uuid.New().String(), topic: topic}
	km.ref = ref
	imt.buf.Add(ref.key, &km)
	if imt.c != nil {
		close(imt.c)
		imt.c = nil
	}
	return nil
}

func (imt *imTopic) commit(key string) {
	imt.lock.Lock()
	defer imt.lock.Unlock()
	it := imt.buf.Iterator()
	defer it.Close()
	for it.HasNext() {
		e, _ := it.Next()
		if e.Key == key {
			imt.buf.Remove(e.Key)
			break
		}
		imt.buf.Remove(e.Key)
	}
}

func (imt *imTopic) reset() {
	imt.lock.Lock()
	defer imt.lock.Unlock()

	imt.it.Close()
	imt.it = imt.buf.Iterator()
	if imt.c != nil {
		close(imt.c)
		imt.c = nil
	}
}

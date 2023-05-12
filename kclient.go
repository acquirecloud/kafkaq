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
	"github.com/acquirecloud/golibs/container"
	"github.com/acquirecloud/golibs/errors"
	"github.com/acquirecloud/golibs/logging"
	"github.com/twmb/franz-go/pkg/kgo"
	"reflect"
	"sync"
	"sync/atomic"
)

type (
	KClientConfig struct {
		Brokers []string
		GroupID string
	}

	// kclient is the Kafka client which implements kafkaReadWriter for a consumer Group
	kclient struct {
		cfg    KClientConfig
		fgcs   atomic.Value // fgClients
		lock   sync.Mutex
		closed bool
	}

	mref struct {
		fgc *fgClient
		r   *kgo.Record
	}

	fgClients map[string]*fgClient

	// fgClient is a wrapper around franz-go kafka client
	fgClient struct {
		it          *kgo.FetchesRecordIter
		cl          atomic.Value // *kgo.Client
		lastCmtdRed *kgo.Record
		log         logging.Logger
		lock        sync.Mutex
	}
)

var _ kafkaReadWriter = (*kclient)(nil)

func newKClient(cfg KClientConfig) *kclient {
	kc := new(kclient)
	var err error
	kc.fgcs.Store(make(fgClients))
	kc.cfg = cfg
	if err != nil {
		panic(fmt.Sprintf("could not creat kclient readers cached: %s", err.Error()))
	}
	return kc
}

func (kc *kclient) Close() error {
	kc.lock.Lock()
	defer kc.lock.Unlock()
	if kc.closed {
		return errors.ErrClosed
	}
	kc.closed = true
	for _, c := range kc.fgcs.Load().(fgClients) {
		c.close(context.Background())
	}
	kc.fgcs.Store(make(fgClients))
	return nil
}

func (kc *kclient) read(ctx context.Context, topic string) (kafkaMessage, error) {
	c, err := kc.getClient(topic)
	if err != nil {
		return kafkaMessage{}, err
	}
	return c.getRecord(ctx)
}

func (kc *kclient) getClient(topic string) (*fgClient, error) {
	clnts := kc.fgcs.Load().(fgClients)
	if c, ok := clnts[topic]; ok {
		return c, nil
	}

	kc.lock.Lock()
	defer kc.lock.Unlock()
	clnts = kc.fgcs.Load().(fgClients)
	if c, ok := clnts[topic]; ok {
		return c, nil
	}
	if kc.closed {
		return nil, errors.ErrClosed
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kc.cfg.Brokers...),
		kgo.ConsumerGroup(kc.cfg.GroupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("could not create client for topic=%s: %w", topic, err)
	}
	c := &fgClient{
		log: logging.NewLogger("franz-client." + topic),
	}
	c.cl.Store(cl)
	clnts = container.CopyMap(clnts)
	clnts[topic] = c
	kc.fgcs.Store(clnts)
	return c, nil
}

func (kc *kclient) commit(ctx context.Context, km kafkaMessage) error {
	mref, ok := km.ref.(mref)
	if !ok {
		return fmt.Errorf("unknown message, could not cast to mref type: %s", reflect.TypeOf(km.ref))
	}
	mref.fgc.commit(mref.r)
	return nil
}

func (kc *kclient) write(ctx context.Context, topic string, km kafkaMessage) error {
	c, err := kc.getClient(topic)
	if err != nil {
		return err
	}
	cl := c.cl.Load().(*kgo.Client)
	if cl == nil {
		return errors.ErrClosed
	}
	record := &kgo.Record{Topic: topic, Value: []byte(km.task), Key: []byte(km.key)}
	cl.Produce(ctx, record, nil)
	return nil
}

func (fc *fgClient) getRecord(ctx context.Context) (kafkaMessage, error) {
	var it *kgo.FetchesRecordIter
	for {
		fc.lock.Lock()
		if it != nil {
			fc.it = it
		}
		if fc.it != nil {
			r := fc.it.Next()
			if fc.it.Done() {
				fc.it = nil
			}
			fc.lock.Unlock()
			return kafkaMessage{key: string(r.Key), task: r.Value, ref: mref{fgc: fc, r: r}}, nil
		}
		fc.lock.Unlock()

		for it == nil {
			cl := fc.cl.Load().(*kgo.Client)
			if cl == nil {
				return kafkaMessage{}, errors.ErrClosed
			}
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				fc.log.Warnf("fail to call CommitUncommittedOffsets(): %s", err.Error())
			}
			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				// All errors are retried internally when fetching, but non-retriable errors are
				// returned from polls so that users can notice and take action.
				fc.log.Errorf("got a bunch of error in PollFetches, will return only the first one: %v", errs)
				return kafkaMessage{}, errs[0].Err
			}
			it = fetches.RecordIter()
			if it.Done() {
				if ctx.Err() != nil {
					return kafkaMessage{}, ctx.Err()
				}
				it = nil
			}
		}
	}
}

func (fc *fgClient) commit(r *kgo.Record) {
	fc.lock.Lock()
	defer fc.lock.Unlock()
	cl := fc.cl.Load().(*kgo.Client)
	if cl == nil {
		fc.log.Warnf("commit after close, ignoring it for r=%v", r)
		return
	}
	// which one is bigger
	if fc.lastCmtdRed == nil || fc.lastCmtdRed.LeaderEpoch < r.LeaderEpoch || (fc.lastCmtdRed.LeaderEpoch == r.LeaderEpoch && fc.lastCmtdRed.Offset < r.Offset) {
		fc.lastCmtdRed = r
	}
}

func (fc *fgClient) close(ctx context.Context) {
	fc.lock.Lock()
	r := fc.lastCmtdRed
	cl := fc.cl.Load().(*kgo.Client)
	fc.lastCmtdRed = nil
	var nilCl *kgo.Client
	fc.cl.Store(nilCl)
	fc.lock.Unlock()

	if r != nil {
		if err := cl.CommitRecords(ctx, r); err != nil {
			fc.log.Errorf("close(): could not commit record: %s", err.Error())
		}
	}
	cl.Close()
}

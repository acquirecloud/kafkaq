package kafkaq

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/acquirecloud/golibs/cast"
	"github.com/acquirecloud/golibs/container"
	"github.com/acquirecloud/golibs/errors"
	"github.com/segmentio/kafka-go"
)

type (
	KClientConfig struct {
		Brokers []string
		GroupID string
	}

	// kclient is the Kafka client which implements kafkaReadWriter for a consumer Group
	kclient struct {
		cfg     KClientConfig
		readers atomic.Value
		writers atomic.Value
		lock    sync.Mutex
		closed  bool
	}

	mref struct {
		r *kafka.Reader
		m kafka.Message
	}

	mpReaders map[string]*kafka.Reader
	mpWriters map[string]*kafka.Writer
)

var _ kafkaReadWriter = (*kclient)(nil)

func newKClient(cfg KClientConfig) *kclient {
	kc := new(kclient)
	var err error
	kc.readers.Store(make(mpReaders))
	kc.writers.Store(make(mpWriters))
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
	for _, r := range kc.readers.Load().(mpReaders) {
		r.Close()
	}
	for _, w := range kc.writers.Load().(mpWriters) {
		w.Close()
	}
	return nil
}

func (kc *kclient) read(ctx context.Context, topic string) (kafkaMessage, error) {
	r, err := kc.getReader(topic)
	if err != nil {
		return kafkaMessage{}, err
	}
	m, err := r.FetchMessage(ctx)
	if err != nil {
		return kafkaMessage{}, err
	}
	return kafkaMessage{key: cast.ByteArrayToString(m.Key), task: Task(m.Value), ref: mref{r: r, m: m}}, nil
}

func (kc *kclient) getReader(topic string) (*kafka.Reader, error) {
	rdrs := kc.readers.Load().(mpReaders)
	if r, ok := rdrs[topic]; ok {
		return r, nil
	}

	kc.lock.Lock()
	defer kc.lock.Unlock()
	rdrs = kc.readers.Load().(mpReaders)
	if r, ok := rdrs[topic]; ok {
		return r, nil
	}
	if kc.closed {
		return nil, errors.ErrClosed
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        kc.cfg.Brokers,
		GroupID:        kc.cfg.GroupID,
		Topic:          topic,
		CommitInterval: time.Microsecond,
	})
	rdrs = container.CopyMap(rdrs)
	rdrs[topic] = r
	kc.readers.Store(rdrs)
	return r, nil
}

func (kc *kclient) commit(ctx context.Context, km kafkaMessage) error {
	mref, ok := km.ref.(mref)
	if !ok {
		return fmt.Errorf("unknown message, could not cast to mref type: %s", reflect.TypeOf(km.ref))
	}
	return mref.r.CommitMessages(ctx, mref.m)
}

func (kc *kclient) write(ctx context.Context, topic string, km kafkaMessage) error {
	w, err := kc.getWriter(topic)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{Key: cast.StringToByteArray(km.key), Value: km.task})
}

func (kc *kclient) getWriter(topic string) (*kafka.Writer, error) {
	wtrs := kc.writers.Load().(mpWriters)
	if w, ok := wtrs[topic]; ok {
		return w, nil
	}

	kc.lock.Lock()
	defer kc.lock.Unlock()
	wtrs = kc.writers.Load().(mpWriters)
	if w, ok := wtrs[topic]; ok {
		return w, nil
	}
	if kc.closed {
		return nil, errors.ErrClosed
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(kc.cfg.Brokers...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		BatchTimeout:           time.Microsecond,
	}
	wtrs = container.CopyMap(wtrs)
	wtrs[topic] = w
	kc.writers.Store(wtrs)
	return w, nil
}

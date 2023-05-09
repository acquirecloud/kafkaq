package kafkaq

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/acquirecloud/coreapis/golibs/cast"
	context2 "github.com/acquirecloud/coreapis/golibs/context"
	"github.com/acquirecloud/coreapis/golibs/errors"
	"github.com/acquirecloud/coreapis/golibs/logging/v1"
	"github.com/acquirecloud/coreapis/kvs"
	"github.com/google/uuid"
	"sync"
	"time"
)

type (
	// QueueConfig struct defines the configuration which will be used by Queue object to
	// process tasks
	QueueConfig struct {
		// Topic the main Kafka topic where all initial tasks are placed
		Topic string

		// WaitTopic is the back-up topic, where tasks that are scheduled for a processing
		// are placed to have an ability to re-execute them in case of a retry is needed
		WaitTopic string

		// Timeout defines the timeout between the scheduling a job execution and the
		// job retry execution in case of the job is not done.
		Timeout time.Duration

		// DegradationTimeout defines the timeout how long a state of the job
		// maybe stored in the system. If a task will be re-tried with the period of
		// time longer that this timeout, the system cannot guarantee the once job execution
		// due to the lost of the job state. This case a task maybe processed more than once
		DegradationTimeout time.Duration
	}

	// queue supports both TaskPubslisher and the Queue interfaces. Please take into account
	// that the object supports linker.Initializer and linker.Shutdowner interfaces and it must be
	// initialized and shutdowned.
	queue struct {
		kvs      kvs.Storage
		kfClient kafkaReadWriter
		cfg      QueueConfig
		done     chan struct{}
		jobCh    chan jobCh
		cpool    sync.Pool
		logger   logging.Logger
		jobExpMs int64
		mctx     context.Context
	}

	publisher struct {
		kfClient kafkaReadWriter
		done     chan struct{}
		mctx     context.Context
		topic    string
	}

	jobCh chan *job

	job struct {
		t      Task
		kvsKey string
		q      *queue
	}

	tqRec []byte

	kafkaMessage struct {
		key  string
		task Task
		ref  any
	}

	// kafkaReadWriter interface provides access to the "Kafka" client, which exposes
	// read and wirte messages from a topic and an commit messages procedure
	kafkaReadWriter interface {
		read(ctx context.Context, topic string) (kafkaMessage, error)
		commit(ctx context.Context, km kafkaMessage) error
		write(ctx context.Context, topic string, km kafkaMessage) error
	}
)

var _ Queue = (*queue)(nil)
var _ TaskPublisher = (*queue)(nil)
var _ TaskPublisher = (*publisher)(nil)

// GetDefaultConfig returns a default config for the Queue object
func GetDefaultConfig() QueueConfig {
	return QueueConfig{
		Topic:              "kqMain",
		WaitTopic:          "kqTimeout",
		Timeout:            time.Minute,
		DegradationTimeout: time.Hour,
	}
}

// NewQueue allows to construct the new queue object. The function expects kvs.Storage and kafkaReadWriter
// to be provided to construct the queue object.
func NewQueue(kvs kvs.Storage, kfClient kafkaReadWriter, cfg QueueConfig) *queue {
	q := new(queue)
	q.kvs = kvs
	q.kfClient = kfClient
	q.cfg = cfg
	q.cpool = newCPool()
	q.done = make(chan struct{})
	q.mctx = context2.WrapChannel(q.done)
	q.jobCh = make(chan jobCh)
	q.logger = logging.NewLogger("kafkaq.queue")
	q.jobExpMs = int64(5 * time.Second / time.Millisecond)
	return q
}

// NewPublisher returns the publisher object
func NewPublisher(kfClient kafkaReadWriter, topic string) *publisher {
	p := new(publisher)
	p.done = make(chan struct{})
	p.mctx = context2.WrapChannel(p.done)
	p.topic = topic
	p.kfClient = kfClient
	return p
}

func newCPool() sync.Pool {
	return sync.Pool{New: func() any {
		return make(jobCh)
	}}
}

func (q *queue) Init(_ context.Context) error {
	q.logger.Infof("Init()")
	go q.mainTopicReader(q.cfg.Topic)
	go q.waitTopicReader(q.cfg.WaitTopic)
	return nil
}

func (q *queue) Shutdown() {
	q.logger.Infof("Shutdown()")
	close(q.done)
}

func (q *queue) mainTopicReader(topic string) {
	q.logger.Infof("entering mainTopicReader, topic=%s", topic)
	defer q.logger.Infof("leaving mainTopicReader, topic=%s", topic)
	for q.mctx.Err() == nil {
		km, err := q.kfClient.read(q.mctx, topic)
		if err != nil {
			q.logger.Warnf("could not read message from the topic: %s", err.Error())
			context2.Sleep(q.mctx, time.Second)
			continue
		}
		if err := q.onMessage(q.mctx, topic, km); err != nil {
			q.logger.Infof("error in onMessage, exiting: %s", err.Error())
			return
		}
		if err := q.kfClient.commit(q.mctx, km); err != nil {
			q.logger.Errorf("could not commit message key=%s, topic=%s: %s", km.key, topic, err.Error())
		}
	}
}

func (q *queue) waitTopicReader(topic string) {
	q.logger.Infof("entering waitTopicReader")
	defer q.logger.Infof("leaving waitTopicReader")
	for q.mctx.Err() == nil {
		km, err := q.kfClient.read(q.mctx, topic)
		if err != nil {
			q.logger.Warnf("could not read message from the topic: %s", err.Error())
			context2.Sleep(q.mctx, time.Second)
			continue
		}
		if err := q.onMessageWaitQueue(q.mctx, km); err != nil {
			q.logger.Infof("error in onMessageWaitQueue")
			return
		}
		if err := q.onMessage(q.mctx, topic, km); err != nil {
			q.logger.Infof("error in onMessage, exiting: %s", err.Error())
			return
		}
		if err := q.kfClient.commit(q.mctx, km); err != nil {
			q.logger.Errorf("could not commit message key=%s, topic=%s: %s", km.key, topic, err.Error())
		}
	}
}

// OnMessage main topic message receiver, the function returns error only if the context is closed.
func (q *queue) onMessage(ctx context.Context, topic string, km kafkaMessage) error {
	var j *job
	var jobExp int64
	for {
	L1:
		// the select below waits either the job consumer or the context is closed. The goroutine may hang
		// there for a long time.
		select {
		case <-ctx.Done():
			q.logger.Infof("onMessage(): the calling context for the topic=%s, key=%s is closed", topic, km.key)
			return ctx.Err()
		case c := <-q.jobCh:
			// ok, we got a job consumer, lets check whether we already have the job j or need to create a new one
			if j == nil {
				var err error
				if topic == q.cfg.Topic {
					j, err = q.processMainTopic(ctx, km)
				} else {
					j, err = q.processWaitTopic(ctx, km)
				}
				if err != nil {
					// Signal the task listener that the channel is closed
					close(c)
					q.logger.Tracef("could not process kafka message key=%s, will try again to get the job: %s", km.key, err.Error())
					goto L1
				}
				if j == nil {
					// skipping the message. It is correct, but no job required, commit it
					close(c)
					return nil
				}
				// we will hold the job j here not longer than q.jobExpMs.
				jobExp = time.Now().UnixMilli() + q.jobExpMs
			} else if time.Now().UnixMilli() > jobExp {
				// HIGHLY UNLIKELY: The situation here indicates about some raise between the GetJob() function cycle
				// the cycle in the function. if the job j cannot be delivered to the consumer longer than q.jobExpMs,
				// we just drop j, and it will be delievered in the wait queue timeout.
				q.logger.Warnf("could not distribute job for the %s timeout, giving up with it.", time.Duration(q.jobExpMs)*time.Millisecond)
				close(c)
				return nil
			}

			select {
			case c <- j:
				// everything is fine, GetJob() got it!
				return nil
			default:
				// the task listener is cancelled for whatever reason or has not started to listen c. Closing the channel.
				// We have the job j ready, so let's try to distribute it again.
				close(c)
			}
		}
	}
}

// onMessageWaitQueue waits until time for the msg comes, but no longer than q.maxWaitTimeout.
// This function is called for processing messages from wait queue only. If the message payload
// indicates that it has timeout bigger than configured, the timeout will be shortened to the
// q.maxWaitTimeout. The function returns error only if the context is closed
func (q *queue) onMessageWaitQueue(ctx context.Context, km kafkaMessage) error {
	wqt := tqRec(km.task)
	ts := wqt.tsMillis()
	nowMillis := time.Now().UnixMilli()
	if ts > nowMillis {
		to := time.Millisecond * time.Duration(ts-nowMillis)
		if to > q.cfg.Timeout {
			q.logger.Warnf("misconfiguration: the message timeout is %s, but configured is %s, setting up it to %s to avoid the processing blockage.", to, q.cfg.Timeout, q.cfg.Timeout)
			to = q.cfg.Timeout
		}
		if err := context2.Sleep(ctx, to); err != nil {
			q.logger.Debugf("onMessageWaitQueue(): the context is clossed while sleeping: key=%s", km.key)
			return err
		}
	}
	return nil
}

func (q *queue) processMainTopic(ctx context.Context, km kafkaMessage) (*job, error) {
	ver := uuid.New()
	// prepare the task for waiting queue
	wqt := newTQRec(ver, time.Now().Add(q.cfg.Timeout).UnixMilli(), km.task)
	if err := q.kfClient.write(ctx, q.cfg.WaitTopic, kafkaMessage{key: km.key, task: Task(wqt)}); err != nil {
		return nil, err
	}

	// remember the active task version in kvs
	kk := kvsKey(km.key)
	r := kvs.Record{Key: kk, Value: wqt.verBuf(), ExpiresAt: cast.Ptr(time.Now().Add(q.cfg.DegradationTimeout))}
	if _, err := q.kvs.Create(ctx, r); err != nil {
		if errors.Is(err, errors.ErrExist) {
			// ignore the message
			return nil, nil
		}
		return nil, err
	}
	return &job{t: km.task, kvsKey: kk, q: q}, nil
}

// processWaitTopic returns task from the wait queue. It will return an error if the message
// was not processed properly and should be retried. It may return nil for the job and nil for
// the error, it means that the message is processed, maybe committed, but no task for the execution
func (q *queue) processWaitTopic(ctx context.Context, km kafkaMessage) (*job, error) {
	wqt := tqRec(km.task)
	kk := kvsKey(km.key)
	r, err := q.kvs.Get(ctx, kk)
	if err != nil {
		if errors.Is(err, errors.ErrNotExist) {
			// ignore the message
			return nil, nil
		}
		return nil, err
	}
	if bytes.Compare(wqt.verBuf(), r.Value) != 0 {
		// ignore the message
		return nil, nil
	}

	ver := uuid.New()
	wqt = newTQRec(ver, time.Now().Add(q.cfg.Timeout).UnixMilli(), wqt.taskUnsafe())
	if err = q.kfClient.write(ctx, q.cfg.WaitTopic, kafkaMessage{key: km.key, task: Task(wqt)}); err != nil {
		return nil, err
	}
	r.ExpiresAt = cast.Ptr(time.Now().Add(q.cfg.DegradationTimeout))
	r.Value = wqt.verBuf()
	_, err = q.kvs.CasByVersion(ctx, r)
	if errors.Is(err, errors.ErrConflict) || errors.Is(err, errors.ErrNotExist) {
		return nil, nil
	}
	return &job{t: wqt.task(), kvsKey: kk, q: q}, err
}

// GetJob is the Queue interface implementation
func (q *queue) GetJob(ctx context.Context) (Job, error) {
	for {
		c := q.cpool.Get().(jobCh)
		select {
		case <-ctx.Done():
			q.cpool.Put(c)
			return nil, ctx.Err()
		case <-q.done:
			// game over, queue is shut down
			q.cpool.Put(c)
			return nil, errors.ErrClosed
		case q.jobCh <- c:
			// good, the writer picked up the channel c by reading from the q.jobCh
		}

		select {
		case <-ctx.Done():
			// writer will close the channel itself
			return nil, ctx.Err()
		case <-q.done:
			// game over, the queue is shut down
			return nil, errors.ErrClosed
		case j, ok := <-c:
			if ok {
				// reuse the channel for further reads
				q.cpool.Put(c)
				return j, nil
			}
			// writer closed the channel, try the loop again
		}
	}
}

// Publish is the part of TaskPublisher implementation
func (q *queue) Publish(task Task) error {
	return q.kfClient.write(q.mctx, q.cfg.Topic, kafkaMessage{key: uuid.NewString(), task: task})
}

// completeJob marks the job completed
func (q *queue) completeJob(kk string) error {
	r, err := q.kvs.Get(context.Background(), kk)
	if err != nil {
		return err
	}
	ver := uuid.New()
	r.Value = ver[:]
	r.ExpiresAt = cast.Ptr(time.Now().Add(q.cfg.DegradationTimeout))
	_, err = q.kvs.Put(context.Background(), r)
	return err
}

func (p *publisher) Shutdown() {
	close(p.done)
}

func (p *publisher) Publish(task Task) error {
	return p.kfClient.write(p.mctx, p.topic, kafkaMessage{key: uuid.NewString(), task: task})
}

func (j *job) Task() Task {
	return j.t
}

func (j *job) Done() error {
	return j.q.completeJob(j.kvsKey)
}

func kvsKey(key string) string {
	return fmt.Sprintf("/kqtasks/%s", key)
}

func newTQRec(ver uuid.UUID, tsMilli int64, t Task) tqRec {
	buf := make([]byte, len(ver)+len(t)+8)
	copy(buf, ver[:])
	binary.BigEndian.PutUint64(buf[uuidLen:], uint64(tsMilli))
	copy(buf[uuidLen+8:], t)
	return buf
}

func (tr tqRec) ver() uuid.UUID {
	var ver uuid.UUID
	copy(ver[:], tr)
	return ver
}

func (tr tqRec) verBuf() []byte {
	if tr.invalid() {
		return nil
	}
	return tr[:uuidLen]
}

func (tr tqRec) tsMillis() int64 {
	if tr.invalid() {
		return 0
	}
	return int64(binary.BigEndian.Uint64(tr[uuidLen:]))
}

func (tr tqRec) taskUnsafe() Task {
	if tr.invalid() {
		return nil
	}
	return Task(tr[uuidLen+8:])
}

func (tr tqRec) task() Task {
	if tr.invalid() {
		return nil
	}
	res := make([]byte, len(tr)-uuidLen-8)
	copy(res, tr[uuidLen+8:])
	return Task(res)
}

var dummyUUID uuid.UUID
var uuidLen = len(dummyUUID)

func (tr tqRec) invalid() bool {
	return len(tr) < uuidLen+8
}

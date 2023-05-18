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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/acquirecloud/golibs/cast"
	context2 "github.com/acquirecloud/golibs/context"
	"github.com/acquirecloud/golibs/errors"
	"github.com/acquirecloud/golibs/kvs"
	"github.com/acquirecloud/golibs/kvs/inmem"
	kvsRedis "github.com/acquirecloud/golibs/kvs/redis"
	"github.com/acquirecloud/golibs/logging"
	"github.com/acquirecloud/kafkaq"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"sync"
	"time"
)

type (
	// PublisherConfig struct defines the queue publisher configuration
	PublisherConfig struct {
		// Brokers contains the list of Kafka brokers
		Brokers []string

		// GroupID is the name of consumer group which will be used to access to the kafka topics
		GroupID string

		// Topic the main Kafka topic where all initial tasks are placed
		Topic string
	}

	// QueueConfig struct defines the configuration which will be used by the queue object to
	// process tasks. The QueueConfig extends the PublisherConfig
	QueueConfig struct {
		PublisherConfig

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

	jobCh chan *job

	job struct {
		t   kafkaq.Task
		q   *queue
		jid string
	}

	// tqRec task queue record. the object is written into the waiting queue
	// tqRec: [version(16 bytes)|nextExecTimeMillis(8bytes)|task data (variable)]
	tqRec []byte

	// kRec is the kvs.Storage record
	// kRec has the following format: [version(16 bytes)|reschedules(4 bytes)|nextExecTimeMillis(8bytes)]
	kRec []byte

	kafkaMessage struct {
		key  string
		task kafkaq.Task
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

var _ kafkaq.Queue = (*queue)(nil)
var _ kafkaq.TaskPublisher = (*queue)(nil)
var _ kafkaq.JobController = (*queue)(nil)

func GetDefaultPublisherConfig() PublisherConfig {
	return PublisherConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "kqMain",
		GroupID: "test",
	}
}

// GetDefaultQueueConfig returns a default config for the Queue object
func GetDefaultQueueConfig() QueueConfig {
	return QueueConfig{
		PublisherConfig:    GetDefaultPublisherConfig(),
		WaitTopic:          "kqTimeout",
		Timeout:            time.Minute,
		DegradationTimeout: time.Hour,
	}
}

// NewKafkaRedis returns the queue object connected to kafka and redis by the configurations
// provided. Please be aware that the result object should be initialized and closed
// via Init() and Shutdown() functions calls respectively.
//
// NOTE: the result object *queue supports both JobController, Queue and TaskPublisher interfaces
func NewKafkaRedis(cfg QueueConfig, redisOpts *redis.Options) *queue {
	kvs := kvsRedis.New(redisOpts)
	kc := newKClient(kClientConfig{brokers: cfg.Brokers, groupID: cfg.GroupID})
	q := newQueue(kvs, kc, cfg)
	go func() {
		select {
		case <-q.mctx.Done():
			kc.Close()
		}
	}()
	return q
}

// NewKafkaKVS returns  the queue object connected to kafka and kvs.Storage by the configurations
// provided. Please be aware that the result object should be initialized and closed
// via Init() and Shutdown() functions calls respectively.
//
// NOTE: the result object *queue supports JobController, Queue and TaskPublisher interfaces
func NewKafkaKVS(cfg QueueConfig, storage kvs.Storage) *queue {
	kc := newKClient(kClientConfig{brokers: cfg.Brokers, groupID: cfg.GroupID})
	return newQueue(storage, kc, cfg)
}

// NewInMem creates the queue with the inmem clients. The object may be used
// for testing.
func NewInMem(cfg QueueConfig) *queue {
	kvs := inmem.New()
	krw := newIMClient()
	return newQueue(kvs, krw, cfg)
}

// newQueue allows to construct the new queue object. The function expects kvs.Storage and kafkaReadWriter
// to be provided to construct the queue object.
func newQueue(kvs kvs.Storage, kfClient kafkaReadWriter, cfg QueueConfig) *queue {
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
					context2.Sleep(ctx, time.Millisecond*100) // try in 100ms with the same km
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

// processMainTopic tries to write the km state into KVS and to submit the km for future
// processing. Any returned error indicates that the km must be re-tried late, but no
// new messages must be tried, before the function starts to return nil for the error.
//
// The function may return nil for both result. It indicates that nothing should be done
// with the km, but next message may be tried.
func (q *queue) processMainTopic(ctx context.Context, km kafkaMessage) (*job, error) {
	ver := uuid.New()
	// prepare the task for waiting queue
	nextExecAt := time.Now().Add(q.cfg.Timeout).UnixMilli()
	wqt := newTQRec(ver, nextExecAt, km.task)
	if err := q.kfClient.write(ctx, q.cfg.WaitTopic, kafkaMessage{key: km.key, task: kafkaq.Task(wqt)}); err != nil {
		return nil, err
	}

	// remember the active task in the kvs
	kr := newKRec(ver, nextExecAt, 0)
	kk := kvsKey(km.key)
	r := kvs.Record{Key: kk, Value: kr, ExpiresAt: cast.Ptr(time.Now().Add(q.cfg.DegradationTimeout))}
	if _, err := q.kvs.Create(ctx, r); err != nil {
		if errors.Is(err, errors.ErrExist) {
			// ignore the message
			return nil, nil
		}
		return nil, err
	}
	return &job{t: km.task, q: q, jid: km.key}, nil
}

// processWaitTopic returns job for the message from the wait queue. It will return an error if the message
// was not processed properly and should be re-tried. It may return nil for the job and nil for
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
	kr := kRec(r.Value)
	if bytes.Compare(wqt.verBuf(), kr.verBuf()) != 0 {
		// ignore the message
		return nil, nil
	}

	ver := uuid.New()
	nextRun := time.Now().Add(q.cfg.Timeout).UnixMilli()
	wqt = newTQRec(ver, nextRun, wqt.taskUnsafe())
	if err = q.kfClient.write(ctx, q.cfg.WaitTopic, kafkaMessage{key: km.key, task: kafkaq.Task(wqt)}); err != nil {
		return nil, err
	}
	r.ExpiresAt = cast.Ptr(time.Now().Add(q.cfg.DegradationTimeout))
	r.Value = kr.set(ver, nextRun, kr.reschedules()+1)
	_, err = q.kvs.CasByVersion(ctx, r)
	if errors.Is(err, errors.ErrConflict) || errors.Is(err, errors.ErrNotExist) {
		return nil, nil
	}
	return &job{t: wqt.task(), q: q, jid: km.key}, err
}

// GetJob is the Queue interface implementation
func (q *queue) GetJob(ctx context.Context) (kafkaq.Job, error) {
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
func (q *queue) Publish(task kafkaq.Task) (string, error) {
	id := uuid.NewString()
	return id, q.kfClient.write(q.mctx, q.cfg.Topic, kafkaMessage{key: id, task: task})
}

// Get is the part of JobController
func (q *queue) Get(jid string) (kafkaq.JobInfo, error) {
	kk := kvsKey(jid)
	r, err := q.kvs.Get(context.Background(), kk)
	if err != nil {
		if errors.Is(err, errors.ErrNotExist) {
			// ignore the message
			return kafkaq.JobInfo{ID: jid, Status: kafkaq.JobStatusUnknown}, nil
		}
		return kafkaq.JobInfo{}, err
	}
	kr := kRec(r.Value)
	ji := kafkaq.JobInfo{
		Status:     kafkaq.JobStatusDone,
		ID:         jid,
		Rescedules: kr.reschedules(),
	}
	if kr.nextRunAtMillis() != 0 {
		ji.Status = kafkaq.JobStatusProcessing
		ji.NextExecutionAt = time.UnixMilli(kr.nextRunAtMillis())
	}
	return ji, nil
}

// Cancel is the part of JobController
func (q *queue) Cancel(jid string) (kafkaq.JobInfo, error) {
	kk := kvsKey(jid)
	r, err := q.kvs.Get(context.Background(), kk)
	if err != nil {
		q.logger.Debugf("cancelling jid=%s, but got an error: %s", jid, err.Error())
	}
	kr := kRec(r.Value)
	kr = kr.set(uuid.New(), 0, kr.reschedules())
	r.Key = kk
	r.ExpiresAt = cast.Ptr(time.Now().Add(q.cfg.DegradationTimeout))
	r.Value = kr
	_, err = q.kvs.Put(context.Background(), r)
	return kafkaq.JobInfo{
		Status:     kafkaq.JobStatusDone,
		ID:         jid,
		Rescedules: kr.reschedules(),
	}, err
}

func (j *job) ID() string {
	return j.jid
}

func (j *job) Task() kafkaq.Task {
	return j.t
}

func (j *job) Done() error {
	_, err := j.q.Cancel(j.jid)
	return err
}

func kvsKey(key string) string {
	return fmt.Sprintf("/kqtasks/%s", key)
}

func newTQRec(ver uuid.UUID, tsMilli int64, t kafkaq.Task) tqRec {
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

func (tr tqRec) taskUnsafe() kafkaq.Task {
	if tr.invalid() {
		return nil
	}
	return kafkaq.Task(tr[uuidLen+8:])
}

func (tr tqRec) task() kafkaq.Task {
	if tr.invalid() {
		return nil
	}
	res := make([]byte, len(tr)-uuidLen-8)
	copy(res, tr[uuidLen+8:])
	return res
}

var dummyUUID uuid.UUID
var uuidLen = len(dummyUUID)

func (tr tqRec) invalid() bool {
	return len(tr) < uuidLen+8
}

// newKRec creates the new kRec object
func newKRec(ver uuid.UUID, nextRunAtMillis int64, reschedules int) kRec {
	var kr kRec
	return kr.set(ver, nextRunAtMillis, reschedules)
}

// set allows to update the kr fields. It returns updated object (which may be different, then current)
func (kr kRec) set(ver uuid.UUID, nextRunAtMillis int64, reschedules int) kRec {
	if len(ver)+12 > len(kr) {
		kr = make([]byte, len(ver)+12)
	}
	copy(kr, ver[:])
	binary.BigEndian.PutUint32(kr[uuidLen:], uint32(reschedules))
	binary.BigEndian.PutUint64(kr[uuidLen+4:], uint64(nextRunAtMillis))
	return kr
}

func (kr kRec) ver() uuid.UUID {
	var ver uuid.UUID
	if len(kr) >= uuidLen {
		copy(ver[:], kr)
	}
	return ver
}

func (kr kRec) verBuf() []byte {
	if len(kr) < uuidLen {
		return nil
	}
	return kr[:uuidLen]
}

func (kr kRec) nextRunAtMillis() int64 {
	if len(kr) < uuidLen+12 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(kr[uuidLen+4:]))
}

func (kr kRec) reschedules() int {
	if len(kr) < uuidLen+4 {
		return 0
	}
	return int(binary.BigEndian.Uint32(kr[uuidLen:]))
}

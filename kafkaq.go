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
package kafkaq

import (
	"context"
	"time"
)

type (
	// Task abstraction represents a task which may be published to the queue and then consumed
	// for further processing.
	Task []byte

	// Job is an object that allows to signal the task completion.
	Job interface {
		// Info returns the JobInfo object for the job
		Info() JobInfo

		// Task returns the task for the job.
		Task() Task

		// Done marks the task as processed, so it will not be re-tried again by a timeout.
		// If Done() is called before the task timeout fired, the task will not
		// be executed again. Otherwise, the task can be run in another job.
		// The function may return an error if the request cannot be executed for a whatever reason.
		// For this case the task timeout is still in charge and the task may be re-executed
		// again later.
		Done() error
	}

	// JobController allows to discover information about jobs in the queue
	JobController interface {
		// Get returns the JobInfo for the job ID provided. The function returns
		// error if the JobInfo may not be obtained for whatever reasons, otherwise
		// it will return the job status and some information about it
		Get(jid string) (JobInfo, error)

		// Cancel allows to stop the future job processing by its id. If the job is
		// not returned for processing yet, it will be not, after the call. If the
		// job was already done, nothing will happen. The call may be made
		// in the middle of the job processing, and it will not interrupt it, but it
		// will affect only further job runs if they are scheduled.
		Cancel(jid string) (JobInfo, error)
	}

	// TaskPublisher allows to submit a new task into the queue. It separated from the
	// Queue object cause not all publishers need to consume jobs from the queue.
	TaskPublisher interface {
		// Publish places the Task into the queue. The function returns the
		// new Job ID for the task which will be returned by Queue.GetJob() later
		Publish(Task) (string, error)
	}

	// Queue interface represents the queue object: Tasks may be published to the queue
	// and retrieved as jobs for processing via GetJob() function. Each task stays in the
	// queue until it is done for the processing. When a task is retrieved from the queue
	// the timeout for the task is charged and if the task is not done on the moment when
	// the timeout is fired, a new job for the task will be returned again until the task
	// is done.
	Queue interface {
		// GetJob returns a job for a submitted task. The call will be blocked until
		// the task is found or the context is closed. The function will return error
		// if the task can be retrieved (normally if the ctx is closed)
		GetJob(ctx context.Context) (Job, error)
	}

	// JobStatus contains the status of a job, please see constants below
	JobStatus int

	// JobInfo contains information about a job
	JobInfo struct {
		// ID contains the Job ID
		ID string
		// Status contains the known status for the job
		Status JobStatus
		// Rescedules contains how many times the job was returned by the rescheduling timeout
		Rescedules int
		// NextExecutionAt contains the timestamp when the job will be executed if it will
		// not be done before it.
		NextExecutionAt time.Time
	}
)

const (
	// JobStatusUnknown indicates that the job is not seen yet, or it was
	// done some time ago, so the information about the job is already gone. The
	// State indicates there is no record about the job, it is not quite possible to say
	// whether the job ever existed or not seen yet.
	JobStatusUnknown JobStatus = iota

	// JobStatusProcessing indicates that the job is seen, distributed and not done yet.
	// This state indicates that the job was selected, but it is not done (for whatever reasons)
	// yet. The job can be rescheduled and returned for the next run. The information about when
	// the Job can be run again maybe found in the JobInfo object.
	JobStatusProcessing

	// JobStatusDone indicates that the job existed, it is done, and the record about it still exists
	// in the queue.
	JobStatusDone
)

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
)

type (
	// Task abstraction represents a task which may be published to the queue and then consumed
	// for further processing.
	Task []byte

	// Job is an object that allows to signal the task completion.
	Job interface {
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

	// TaskPublisher allows to submit a new task into the queue. It separated from the
	// Queue object cause not all publishers need to consume jobs from the queue.
	TaskPublisher interface {
		// Publish places the Task into the queue
		Publish(Task) error
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
)

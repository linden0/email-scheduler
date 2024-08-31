
# Email Scheduler

Email scheduler is a job scheduler that sends future emails using the Master/Worker pattern.

## Basic Architecture

- To start, app will insert a document into the task_queue db in the form of `(recipient_email, subject, body, due, status=QUEUED, worker)`
- Workers will then process assigned tasks, and mark as `status=SENT`
## Resiliency
* Each stage has multiple workers integrated together for multiple points of failure
* Master sends heartbeat checks to workers. If a worker is down, it will remove it form registry and reassign its tasks to another worker
* Workers will send heartbeat checks to the master. If a master is down, all workers will attempt to claim the master role. Workers will perform a atomic registry update to try to claim master role, and the one that succeeds is reinitialized as a master
* tasks are processed with exponential retry backoff along with a `max_retry`

## Todo
* Authenticate endpoints so only master and workers can communicate with each other
* improving logging, use the colored logging scheme defined in `helpers.go`

    

# Email Scheduler

Email scheduler is a job scheduler that sends future emails using the Master/Worker pattern.

## Architecture

- The application will insert a document into the task_queue db in the form of (recipient_email, subject, body, send_date, status=READY, worker=None)
- Multiple assignment workers will assign the email to the master and worker by:
  * Claiming the email job by marking the status to PROCESSING using an atomic operation (to avoid race conditions)
  * Assigning the job a worker using consistent hashing for load distribution
- The Master/Workers then move email jobs from the task_queue db to send_queue db.
    * The workers constantly check for jobs assigned to them, and move them using a transaction (since this is a two step operation, copying and deleting, where one could fail)
* Workers claim jobs by marking status CLAIMED in the send_queue and send them via Mailgun, and mark status to FINISHED, again to lock tasks

## Resiliency
* Each stage has multiple workers integrated together for multiple points of failure
* The master will send heartbeat checks to workers. If a worker is down, it will reassign its tasks to another worker
* Workers will send heartbeat checks to the master. If a master is down, all workers will attempt to claim the master role. The one worker that is successful will restart Docker container to become a master

    
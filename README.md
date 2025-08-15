# streamfleet

Customizable Go work queue implementation backed by Redis Streams (or Valkey Streams). No Lua or fancy tricks required.

Tasks placed on the queue are processed once by the worker that can pick up the task quickest.
Completion can be optionally tracked.

## Features

 - Optional task completion notifications
 - Support for Redis Sentinel
 - Automatic retry and worker crash recovery

## Architecture

The application is broken up into two components, coordinated by Redis:

 - The client, which submits (enqueues) tasks to the queue.
   
   Clients can optionally track completion or failure of tasks by listening for notifications about them.
   Whether to send completion notifications is sent along with the task.

 - The server (worker), which accepts work from the queue and processes it.

   In addition to receiving new tasks, workers can claim tasks that have sat idle too long in other workers' pending lists.
   Long-running tasks are kept fresh and not in an idle state as long as the task is pending in the functional server.
   Only servers that have crashed or are unresponsive would have their tasks reclaimed.

   If a task fails while the worker is running, the server will put the task back on the queue and increment its failure count.

Note that servers and clients may exist on the same process; they do not need to be in separate microservices.

If the underlying Redis server is unavailable, the clients will wait until it comes back online.
Tasks submitted to the client stay queued in-memory until Redis is available.

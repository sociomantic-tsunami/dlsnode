# Architecture and use of AIO system in DLS node

## Abstract

DLS node is multithreaded application, where the client requests are served
inside event-driven main thread, and the IO work is deferred to worker threads,
which notify the main thread upon completion of asynchronous IO requests.
This document describes the current system.

## AIO subsystem 

Asynchronous subsystem (shorter: aio) consists of the following components:

- Primitive to suspend/resume client request
- Queue of jobs ready to be executed
- Set of worker threads
- Queue of the user requests ready to resume
- "Scheduler" to resume user requests for which the operations has been completed
- High level API to submit requests for asynchronous IO.

### Primitive to suspend/resume client request

Asynchronous IO is based on completion model. That means that user request
(running in some sort of fiber inside main event-driven thread) will submit a
request, with all the necessary information, buffers and lengths, and it will
be suspended until the requested operation has been completed. Since there are
different implementation of user request fibers, different means are necessary
to suspend and resume the fiber. This is completely independent from the aio
subsystem, requiring only that the request to be suspended implement the
interface ``SuspendableRequestHandler`` (with ``wait`` and ``wake`` methods)
being required. When ``wait`` is called, request fiber should be suspended and
it should be woken up with ``wake``.

### Queue of jobs ready to be executed

Single job to be executed consists of the following fields:

- Command representing IO request type (``pread``/``fsync``/``close``)
- Pointer to a buffer to fill (if applicable)
- Length of the buffer to fill (if applicable)
- File descriptor of a file to perform the IO operation on
- Offset from the beginning of the file to perform the operation (if applicable)
- Pointer to the return value buffer of the operation
- Pointer to the errno buffer of the operation
- Reference to the ``SuspendableRequestHandler`` instance responsible to suspend/resume
  running fiber.

Client is required to fill all applicable fields (i.e. ``fsync`` doesn't require buffer to
run), which is done via high-level API (see below).

The queue consists of the linked list of Job instances, which are reused (to
avoid unnecessary allocation/deallocation) and is managed via private fields
``is_taken`` (indicating if the thread worker is serving this request) and
``is_slot_free`` (indicating that the job in this slot has been completed and
the slot may be reused for another request).

### Worker thread

Worker threads are simple ``pthread`` instances which are created outside Druntime.
Because of that, the operations that they can do are strictly limited,
and ideally, should not be using anything that obtains glibc's locks.

Thread entry point is ``thread_entry_point`` and it consists of infinite loop blocked
on the semaphore which is being signaled from queue of jobs that are ready to run (see
previous section). Whenever there's a job to be run, one of the working threads takes it
and executes whichever command is passed, with parameters that are passed, records the
return status and errno, if applicable, and marks the request as completed, passing the
job to AioScheduler (see the next section) to resume fiber that was waiting for this
operation to complete. After that, thread goes to sleep on the job semaphore, waiting
for the next job to perform.

### Queue of the user requests ready to resume

Similar to the queue of jobs ready to be executed, there's a queue of ``SuspendableRequestHandler``
that reference all the client requests for which the IO has been completed.

### "Scheduler" to resume user requests for which the operations has been completed

Since resuming user requests **must** be done from the main thread (for example,
you must not resume the fiber from worker thread), ``AioScheduler`` is a ``ISelectEvent``
which contains two queues - queue of jobs ready to continue, and queue of jobs that
are currently being woken up. Worker threads put a ``SuspendableRequestHandler`` into
the first queue and perform ``trigger()`` on the scheduler, making ``EpollSelectDispatcher``
call ``handle()`` in the main thread. ``handle()`` grabs the queue of jobs to be waken up,
and calls ``wake()`` for each of them.

### High level API to submit requests for asynchronous IO.

User code, however, should not care about all these details. All user code
needs to do is to create ``AsyncIO`` object which will create all worker threads,
all queues, and everything will be ready. From that point, user code can just
call ``pread``, ``fsync`` and ``close`` with the parameters that are matching C API,
plus the appropriate instance of ``SuspendableRequestHandler`` used to suspend/resume
this request. The ``AsyncIO`` then prepares a job, puts it into the job queue,
triggers the job queue semaphore and calls ``SuspendableRequestHandler.wait``.
ThreadWorker does the job, and ``AioScheduler`` will eventually call
``SuspendableRequestHandler.wake`` continuing the request.


## Usage of AIO subsystem in DLS node

DLS node is using AIO for executing ``pread`` and ``fsync`` requests. Since non-aio calls may block
the calling thread, historically this was the biggest bottleneck in DLS. Both calls are wrapped
around within ``BucketFile`` class (which also contains the file it's operating on) and performing
these operations in blocking mode is also possible (in case of appending to the existing file,
DLS node needs to read the small header to identify the bucket version. This proved to be very
problematic in conjunction with LRU cache and fiber race conditions, so the serialized fibers approach
is used instead, with no noticeable performance degradation, since this usually doesn't happen -
the file for writing is usually created empty).

### Usage of ``pread`` calls

AIO ``pread`` calls are only used in ``BucketFile.readData`` method. While iterating through the bucket
file ``StorageEngineStepIterator`` is indirectly calling this method to get the content of the file.

### Usage of ``fsync`` calls

AIO ``fsync`` calls are used for two purposes:

1. Synchronizing the storage device with the filesystem after flushing, which allows us to
   setup the checkpoint, containing file name and length of the file guaranteed to be on disk,
   since after that call is executed, we do know how much data is at least on the disk

2. For syncing the checkpoint file itself - since this file has to be atomically placed to
   disk ``fsync``/``rename`` trick is used, where the temporary file is committed to disk, and
   then renamed to the permanent location.
/*******************************************************************************

    AioScheduler that is informed when the aio operations are done and ready
    to be continued.

    Scheduler internally has two queues: queue that's accessible to main thread
    and the queue that's accessible to all worker threads. At no point in time
    single queue is available to both main thread and worker threads. As a
    consequence of this, main thread never needs to acquire mutex while
    accessing its queue, and worker threads are only synchronizing between
    themselves while accessing the queue.

    At the beginning, both queues are empty. When worker thread is finished
    processing the request, SuspendableRequestHandler is put into the workers'
    queue and notifies the main thread that there are request that it needs to
    wake up. However, since the main thread may not immediately do this, more
    than one request could end up in the queue. Once main thread is ready to
    wake up the request, it will obtain the mutex and swap these two queues.
    Now, main thread contains queue of all request ready to be woken up, and
    the workers' queue is empty, and main thread now can wake up all the
    request without interruptions from other threads, and worker threads can
    schedule new request to be woken up without interfering with the main
    thread. Once main thread finish processing its queue, and there is at least
    one request in the workers' queue, it will swap the queues again, and the
    same procedure will be performed again.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.util.aio.internal.AioScheduler;

import ocean.io.select.client.SelectEvent;

/// Ditto
class AioScheduler: ISelectEvent
{
    import ocean.sys.ErrnoException;
    import core.sys.posix.pthread;
    import dlsnode.util.aio.SuspendableRequestHandler;
    import dlsnode.util.aio.internal.MutexOps;
    import swarm.neo.util.TreeQueue;
    import dlsnode.util.aio.internal.JobQueue: Job;

    /***************************************************************************

        Mutex protecting request queue

    ***************************************************************************/

    private pthread_mutex_t queue_mutex;

    /***************************************************************************

        See ready_queue's documentation.

    ***************************************************************************/

    private TreeQueue!(Job*)[2] queues;

    /***************************************************************************

        Queue of requests being ready. NOTE: this is just a pointer to a queue
        of requests that are finished processing and they will be woken up
        in the next AioScheduler cycle. On every AioScheduler cycle, pointers
        to `ready_queue` and `waking_queue` are swapped. During a single cycle,
        only the worker threads are accessing ready_queue (inserting references
        to the jobs that are finish) and only the main thread access the
        waking_queue (popping the requests from it and resuming the fibers
        waiting for the IO to complete).

    ***************************************************************************/

    private TreeQueue!(Job*)* ready_queue;

    /***************************************************************************

        Queue of requests that are in process of waking up. For more details,
        see comment for ready_queue, above.

    ***************************************************************************/

    private TreeQueue!(Job*)* waking_queue;

    /***************************************************************************

        Queue of the request whose results should be discarded.

    ***************************************************************************/

    private TreeQueue!(Job*) discarded_queue;

    /***************************************************************************

        Constructor.

        Params:
            exception = exception to throw in case of error

    ***************************************************************************/

    public this (ErrnoException exception)
    {
        exception.enforceRetCode!(pthread_mutex_init).call(
                &this.queue_mutex, null);

        this.ready_queue = &this.queues[0];
        this.waking_queue = &this.queues[1];
    }

    /***************************************************************************

        Mark the request ready to be woken up by scheduler

        Params:
            job = job that has completed, that needs to be finalized and whose
                suspended request should be resumed
            lock_mutex = function or delegate to lock the mutex
            unlock_mutex = function or delegate to unlock the mutex

    ***************************************************************************/

    public void requestReady (MutexOp)(Job* req,
            MutexOp lock_mutex, MutexOp unlock_mutex)
    {
        lock_mutex(&this.queue_mutex);
        scope (exit)
        {
            unlock_mutex(&this.queue_mutex);
        }

        // Check if the results of this request was marked as not needed
        if (!this.discarded_queue.remove(req))
        {
            this.ready_queue.push(req);
            this.trigger();
        }
        else
        {
            req.recycle();
        }
    }

    /***************************************************************************

        Discards the results of the given AIO operation.

        Params:
            req = SuspendableRequestHandler instance that was waiting for results

    ***************************************************************************/

    public void discardResults (Job* req)
    {
        lock_mutex(&this.queue_mutex);
        scope (exit)
        {
            unlock_mutex(&this.queue_mutex);
        }

        // Since we're guarded by lock above, two scenarios might happen:
        // 1) The given ThreadWorker already submitted the results for this
        //    operation, which we will simply remove from the ready queue.
        // 2) No worker thread was assigned for this request, or the operation
        //    was not yet completed. In this case tell the AioScheduler to discard
        //    these results as soon as they arrive
        if (!this.ready_queue.remove(req))
        {
            this.discarded_queue.push(req);
        }
        else
        {
            req.recycle();
        }
    }


    /***************************************************************************

        Called from handle() when the event fires.

        Params:
            n = number of the times this event has been triggered.

    ***************************************************************************/

    protected override bool handle_ ( ulong n )
    {
        this.switchQueues();

        foreach (job; *this.waking_queue)
        {
            auto req = job.suspendable_request_handler;
            assert (req);

            job.finished();
            req.wake();
            job.recycle();
        }

        assert(this.waking_queue.is_empty());

        return true;
    }

    /***************************************************************************

        Switches the queues

    ***************************************************************************/

    private void switchQueues ()
    {
        lock_mutex(&this.queue_mutex);
        scope (exit)
        {
            unlock_mutex(&this.queue_mutex);
        }

        assert (this.waking_queue.is_empty());

        auto tmp = this.waking_queue;
        this.waking_queue = this.ready_queue;
        this.ready_queue = tmp;
    }
}

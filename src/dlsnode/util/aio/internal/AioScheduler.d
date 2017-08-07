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

        Queue of requests being ready

    ***************************************************************************/

    private TreeQueue!(SuspendableRequestHandler)* ready_queue;

    /***************************************************************************

        Queue of requests that are in process of waking up.

    ***************************************************************************/

    private TreeQueue!(SuspendableRequestHandler)* waking_queue;


    /***************************************************************************

        Constructor.

        Params:
            exception = exception to throw in case of error

    ***************************************************************************/

    public this (ErrnoException exception)
    {
        exception.enforceRetCode!(pthread_mutex_init).call(
                &this.queue_mutex, null);

        this.ready_queue = new TreeQueue!(SuspendableRequestHandler);
        this.waking_queue = new TreeQueue!(SuspendableRequestHandler);
    }

    /***************************************************************************

        Mark the request ready to be woken up by scheduler

        Params:
            req = SuspendableRequestHandler to wake the suspended request with
            lock_mutex = function or delegate to lock the mutex
            unlock_mutex = function or delegate to unlock the mutex

    ***************************************************************************/
    public void requestReady (MutexOp)(SuspendableRequestHandler req,
            MutexOp lock_mutex, MutexOp unlock_mutex)
    {
        lock_mutex(&this.queue_mutex);
        scope (exit)
        {
            unlock_mutex(&this.queue_mutex);
        }

        this.ready_queue.push(req);
        this.trigger();
    }


    /***************************************************************************

        Called from handle() when the event fires.

        Params:
            n = number of the times this event has been triggered.

    ***************************************************************************/

    protected override bool handle_ ( ulong n )
    {
        this.switchQueues();

        foreach (item; *this.waking_queue)
        {
            auto req = cast(SuspendableRequestHandler)item;
            assert (req);
            req.wake();
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

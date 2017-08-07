/******************************************************************************

    Module for doing non-blocking reads supported by threads.

    This module contains AsyncIO definition. Intented usage of AsyncIO is to
    perform normally blocking IO calls (disk requests) in fiber-blocking
    manner.

    Fiber wanting to perform a request should submit its request to AsyncIO
    using public interface, passing all the arguments normally used by the
    blocking call and SuspendableRequestHandler instance on which it will be
    blocked.  After issuing the request, request will be put in the queue and
    the fiber will block immidiatelly, giving chance to other fibers to run.

    In the background, fixed amount of worker threads are taking request from
    the queue, and performing it (using blocking call which will in turn block
    this thread). When finished, the worker thread will resume the blocked fiber,
    and block on the semaphore waiting for the next request.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsnode.util.aio.AsyncIO;

import ocean.transition;

import core.stdc.errno;
import core.sys.posix.semaphore;
import core.sys.posix.pthread;
import core.sys.posix.unistd;
import core.stdc.stdint;
import core.stdc.stdio;
import ocean.core.array.Mutation: copy;
import ocean.sys.ErrnoException;
import ocean.io.select.EpollSelectDispatcher;

import dlsnode.util.aio.internal.JobQueue;
import dlsnode.util.aio.internal.ThreadWorker;
import dlsnode.util.aio.internal.MutexOps;
import dlsnode.util.aio.internal.AioScheduler;
import dlsnode.util.aio.SuspendableRequestHandler;

/******************************************************************************

    Class implementing AsyncIO support.

******************************************************************************/

class AsyncIO
{
    /**************************************************************************

        Ernno exception instance

        NOTE: must be thrown and catched only from/in main thread, as it is not
              multithreaded-safe

    **************************************************************************/

    private ErrnoException exception;


    /**************************************************************************

        Job queue

   ***************************************************************************/

    private JobQueue jobs;

    /**************************************************************************

        AioScheduler used to wake the ready jobs.

    **************************************************************************/

    private AioScheduler scheduler;

    /**************************************************************************

        Handles of worker threads.

    **************************************************************************/

    private pthread_t[] threads;


    /**************************************************************************

        Constructor.

        Params:
            epoll = epoll select dispatcher instance
            number_of_threads = number of worker threads to allocate

    **************************************************************************/

    public this (EpollSelectDispatcher epoll, int number_of_threads)
    {

        this.exception = new ErrnoException;

        this.scheduler = new AioScheduler(this.exception);
        this.jobs = new JobQueue(this.exception, this.scheduler);

        // create worker threads
        this.threads.length = number_of_threads;

        foreach (i, tid; this.threads)
        {
            // Create a thread passing this instance as a parameter
            // to thread's entry point
            this.exception.enforceRetCode!(pthread_create).call(&this.threads[i],
                null,
                &thread_entry_point,
                cast(void*)this.jobs);
        }

        epoll.register(this.scheduler);
    }

    /**************************************************************************

        Issues a pread request, blocking the fiber connected to the provided
        suspendable_request_handler until the request finishes.

        This will read buf.length number of bytes from fd to buf, starting
        from offset.

        Params:
            buf = buffer to fill
            fd = file descriptor to read from
            offset = offset in the file to read from
            suspendable_request_handler = SuspendableRequestHandler instance to
                block the fiber on

        Returns:
            number of the bytes read

        Throws:
            ErrnoException with appropriate errno set in case of failure

    **************************************************************************/

    public size_t pread (void[] buf, int fd, size_t offset,
            SuspendableRequestHandler suspendable_request_handler)
    {
        ssize_t ret_val;
        int errno_val;
        auto job = this.jobs.reserveJobSlot(&lock_mutex,
                &unlock_mutex);

        job.recv_buffer.length = buf.length;
        enableStomping(job.recv_buffer);
        job.fd = fd;
        job.suspendable_request_handler = suspendable_request_handler;
        job.offset = offset;
        job.cmd = Job.Command.Read;
        job.ret_val = &ret_val;
        job.errno_val = &errno_val;
        job.user_buffer = buf;
        job.finalize_results = &finalizeRead;

        // Let the threads waiting on the semaphore know that they
        // can start doing single read
        post_semaphore(&this.jobs.jobs_available);

        // Block the fiber
        suspendable_request_handler.wait();

        // At this point, fiber is resumed,
        // check the return value and throw if needed
        if (ret_val == -1)
        {
            throw this.exception.set(errno_val,
                    "pread");
        }

        assert(ret_val >= 0);
        return cast(size_t)ret_val;
    }

    /***************************************************************************

        Finalizes the read request - copies the contents of receive buffer
        to user provided buffer.

        Params:
            job = job to finalize.

    ***************************************************************************/

    private static void finalizeRead (Job* job)
    {
        if (*job.ret_val >= 0)
        {
            auto dest = (job.user_buffer.ptr)[0..*job.ret_val];
            copy(dest, job.recv_buffer[0..*job.ret_val]);
        }
    }

    /**************************************************************************

        Issues a fsync request, blocking the fiber connected to the provided
        suspendable_request_handler until the request finishes.

        Synchronize a file's in-core state with storage device.

        Params:
            fd = file descriptor to perform fsync on
            suspendable_request_handler = SuspendableRequestHandler instance to
                block the fiber on

        Throws:
            ErrnoException with appropriate errno set in the case of failure

    **************************************************************************/

    public void fsync (int fd,
            SuspendableRequestHandler suspendable_request_handler)
    {
        long ret_val;
        int errno_val;

        auto job = this.jobs.reserveJobSlot(&lock_mutex,
                &unlock_mutex);

        job.fd = fd;
        job.suspendable_request_handler = suspendable_request_handler;
        job.cmd = Job.Command.Fsync;
        job.ret_val = &ret_val;
        job.errno_val = &errno_val;
        job.finalize_results = null;

        // Let the threads waiting on the semaphore that they
        // can perform fsync
        post_semaphore(&this.jobs.jobs_available);

        // Block the fiber
        suspendable_request_handler.wait();

        // At this point, fiber is resumed,
        // check the return value and throw if needed
        if (ret_val == -1)
        {
            throw this.exception.set(errno_val,
                    "fsync");
        }
    }

    /**************************************************************************

        Issues a close request, blocking the fiber connected to the provided
        suspendable request handler until the request finishes.

        Synchronize a file's in-core state with storage device.

        Params:
            fd = file descriptor to close
            suspendable_request_handler = SuspendableRequestHandler instance to
                block the caller on

        Throws:
            ErrnoException with appropriate errno set in the case of failure

    **************************************************************************/

    public void close (int fd,
            SuspendableRequestHandler suspendable_request_handler)
    {
        long ret_val;
        int errno_val;

        auto job = this.jobs.reserveJobSlot(&lock_mutex,
                &unlock_mutex);

        job.fd = fd;
        job.suspendable_request_handler = suspendable_request_handler;
        job.cmd = Job.Command.Close;
        job.ret_val = &ret_val;
        job.errno_val = &errno_val;
        job.finalize_results = null;

        post_semaphore(&this.jobs.jobs_available);

        // Block the fiber
        suspendable_request_handler.wait();

        // At this point, fiber is resumed,
        // check the return value and throw if needed
        if (ret_val == -1)
        {
            throw this.exception.set(errno_val,
                    "close");
        }
    }

    /*********************************************************************

        Destroys entire AsyncIO object.
        It's unusable after this point.

        NOTE: this blocks the calling thread

        Throws:
            ErrnoException if one of the underlying system calls
            failed

    *********************************************************************/

    public void destroy ()
    {
        // Stop all workers
        // and wait for all threads to exit
        this.join();

        this.jobs.destroy(this.exception);
    }

    /**************************************************************************

        Indicate worker threads not to take any more jobs.

        Throws:
            ErrnoException if one of the underlying system calls
            failed

    **************************************************************************/

    private void stopAll ()
    {
        this.jobs.stop(&lock_mutex,
                &unlock_mutex);

        // Let all potential threads blocked on semaphore
        // move forward and exit
        for (int i; i < this.threads.length; i++)
        {
            post_semaphore(&this.jobs.jobs_available);
        }
    }

    /**************************************************************************

        Waits for all threads to finish and checks the exit codes.

        Throws:
            ErrnoException if one of the underlying system calls
            failed

    **************************************************************************/

    private void join ()
    {
        // We need to tell threads actually to stop working
        this.stopAll();

        for (int i = 0; i < this.threads.length; i++)
        {
            // Note: no need for mutex guarding this
            // as this is just an array of ids which
            // will not change during the program's lifetime
            void* ret_from_thread;
            int ret = pthread_join(this.threads[i], &ret_from_thread);

            switch (ret)
            {
                case 0:
                    break;
                default:
                    throw this.exception.set(ret, "pthread_join");
                case EDEADLK:
                    assert(false, "Deadlock was detected");
                case EINVAL:
                    assert(false, "Join performed on non-joinable thread" ~
                            " or another thread is already waiting on it");
                case ESRCH:
                    assert(false, "No thread with this tid can be found");
            }

            // Check the return value from the thread routine
            if (cast(intptr_t)ret_from_thread != 0)
            {
                throw this.exception.set(cast(int)ret_from_thread,
                        "thread_method");
            }
        }
    }

    /*********************************************************************

        Helper function for posting the semaphore value
        and checking for the return value

        Params:
            sem = pointer to the semaphore handle


    *********************************************************************/

    private void post_semaphore (sem_t* sem)
    {
        int ret = sem_post(sem);

        switch (ret)
        {
            case 0:
                break;
            default:
                throw this.exception.set(ret, "sem_post");
            case EINVAL:
                assert(false, "The semaphore is not valid");
        }
    }
}

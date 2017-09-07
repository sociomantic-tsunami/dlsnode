/******************************************************************************

    Module containing worker thread implementation of AsyncIO.

    In async IO framework, fixed amount of worker threads are taking request
    from the queue, and performing it (using blocking call which will in turn
    block this thread). When finished, the worker thread will resume the
    blocked fiber, and block on the semaphore waiting for the next request.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsnode.util.aio.internal.ThreadWorker;

import core.stdc.errno;
import core.sys.posix.semaphore;
import core.sys.posix.pthread;
import core.sys.posix.unistd;
import core.stdc.stdint;
import core.stdc.stdio;

import dlsnode.util.aio.internal.JobQueue;

import core.sys.posix.signal: SIGRTMIN;

/**************************************************************************

    Thread's entry point.

**************************************************************************/

extern (C) static void* thread_entry_point (void* job_queue_ptr)
{
    JobQueue jobs = cast(JobQueue)job_queue_ptr;

    // Block all signals delivered to this thread
    sigset_t block_set;

    // Return value is ignored here, as this can fail only
    // because programming error, and we can't use assert here
    cast(void) sigfillset(&block_set);
    cast(void)pthread_sigmask(SIG_BLOCK, &block_set, null);

    // Wait for new jobs and execute them
    while (true)
    {
        // Current job
        Job* job;

        // Wait on the semaphore for new jobs
        while (true)
        {
            auto ret = sem_wait(&(jobs.jobs_available));

            if (ret == -1 && .errno != EINTR)
            {
                exit_thread(-1);
            }
            else if (ret == 0)
            {
                break;
            }
        }

        // Get the next job, if any
        job = jobs.takeFirstNonTakenJob(&thread_lock_mutex,
                &thread_unlock_mutex);

        // No more jobs
        if (job == null)
        {
            break;
        }

        ssize_t ret_value;
        switch (job.cmd)
        {
            case Job.Command.Read:
                ret_value = do_pread(job);
                break;
            case Job.Command.Fsync:
                ret_value = do_fsync(job);
                break;
            case Job.Command.Close:
                ret_value = do_close(job);
                break;
            default:
                break;
        }

        if (ret_value != 0)
        {
            *job.errno_val = .errno;
            *job.ret_val = ret_value;
        }

        // Signal that you have done the job
        signalJobDone(jobs, job);
    }

    return cast(void*)0;
}


/**************************************************************************

    Wrapper around fsync call.

    Params:
        job = job for which the request is executed.

    Returns:
        0 in case of success, -1 in case of failure

**************************************************************************/

private static ssize_t do_fsync (Job* job)
{
    while (true)
    {
        auto ret = .fsync(job.fd);

        if (ret == 0 || .errno != EINTR)
        {
            return ret;
        }
    }
}

/**************************************************************************

    Wrapper around close call.

    Params:
        job = job for which the request is executed.

    Returns:
        0 in case of success, -1 in case of failure

**************************************************************************/

private static ssize_t do_close (Job* job)
{
    ssize_t ret;

    do
    {
        ret = .close(job.fd);
    }
    while (ret != 0 && .errno == EINTR);

    return ret;
}

/**************************************************************************

    Wrapper around pread call.

    Params:
        job = job for which the request is executed.

    Returns:
        number of bytes read, or -1 in case of error.

**************************************************************************/

private static ssize_t do_pread (Job* job)
{
    // Now, do the reading!
    size_t count = 0;
    while (count < job.recv_buffer.length)
    {
        ssize_t read;

        while (true)
        {
            read = .pread(job.fd, job.recv_buffer.ptr + count,
                        job.recv_buffer.length - count, job.offset + count);

            if (read >= 0 || .errno != EINTR)
            {
                break;
            }
        }

        // Check for the error
        if (read < 0)
        {
            return read;
        }

        if (read == 0)
        {
            // No more data
            break;
        }

        count += read;
    }

    return count;
}

/*********************************************************************

    Helper method to exit the thread and raise a signal
    in parent AsyncIO

    This method will signal the main thread that this thread has
    performed the invalid operation from which it can't recover
    and it will exit the current thread with a return code.

*********************************************************************/

private void exit_thread(int return_code)
{
    // getpid is always successful
    int parent_id = getpid();
    pthread_kill(parent_id, SIGRTMIN);
    pthread_exit(cast(void*)return_code);
}

/*********************************************************************

    Method implementing locking the mutex with non-allocating and non
    throwing error handling.

    Since this method will be called from the pthread, we must not
    throw or allocate anything from here.

    Instead, main thread is being signaled and the current thread
    exits with the -1 value.

    Params:
        mutex = mutex to perform the operation on

*********************************************************************/

private void thread_lock_mutex (pthread_mutex_t* mutex)
{
    if (pthread_mutex_lock(mutex) != 0)
    {
        exit_thread(-1);
    }
}

/*********************************************************************

    Method implementing unlocking the mutex with non-allocating and non
    throwing error handling.

    Since this method will be called from the pthread, we must not
    throw or allocate anything from here.

    Instead, main thread is being signaled and the current thread
    exits with the -1 value.

    Params:
        mutex = mutex to perform the operation on

*********************************************************************/

private void thread_unlock_mutex (pthread_mutex_t* mutex)
{
    if (pthread_mutex_unlock(mutex) != 0)
    {
        exit_thread(-1);
    }
}

/**********************************************************************

    Signals that the request has been executed and checks for the
    cancelation

    Params:
        jobs = queue containing jobs to be run
        job = pointer to job containing executed request

**********************************************************************/

private void signalJobDone (JobQueue jobs, Job* job)
{
    jobs.markJobReady(job,
            &thread_lock_mutex, &thread_unlock_mutex);
}

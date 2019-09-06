/******************************************************************************

    Checkpoint service implementation.

    This module provides implementation of the checkpoint service. It provides
    utility for bookkeeping and commiting of the checkpoints. It also provides
    means for parsing the checkpoint file from disk and truncating the buckets
    to the last commited position in the checkpoint log.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsnode.storage.checkpoint.CheckpointService;

import dlsnode.util.aio.JobNotification;
import ocean.core.Test;
import ocean.meta.types.Qualifiers : cstring, istring, mstring;
import ocean.util.log.Logger;

version (UnitTest)
{
    import ocean.stdc.posix.stdlib: mkdtemp;
    import ocean.text.util.StringC;
}

/******************************************************************************

    Static module logger.

******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.storage.checkpoint.CheckpointService");
}

/******************************************************************************

    Service for keeping track of the checkpoints, commiting them periodically
    to disk.

    This class should be used as follows:

    - bucketOpen() method should be called every time a bucket is opened to be
      written to.
    - After each write in the bucket file,
      `checkpoint` should be called with the file length after the flush. This
      will make system remember last position that was flushed to disk.
    - When the bucket is closed, bucketClose() should be call to tell the system
      that it should stop tracking the bucket's position once we store it
      into checkpoint log for the last time.
    - In order to periodically commit the checkpoint file to disk, user should
      call `startService` which will register timer to commit every
      `commit_seconds`.
    - Before doing a commit, checkpoint service will fsync all the data to disk
      making sure that all checkpointed lengths are actually on disk.
    - On a clean exit, `stopService` should be called which will remove
      checkpoint file, indicating that all buckets are shut down in a clean way.

******************************************************************************/

class CheckpointService
{
    import dlsnode.storage.checkpoint.CheckpointFile;
    import OceanPath = ocean.io.Path;
    import ocean.io.device.File;
    import ocean.io.FilePath;
    import ocean.io.select.fiber.SelectFiber;
    import ocean.io.select.client.FiberTimerEvent;
    import ocean.io.select.EpollSelectDispatcher;
    import core.stdc.errno;
    import core.stdc.stdio;
    import ocean.sys.ErrnoException;
    import oceanArray = ocean.core.Array;
    import ocean.core.Array;
    import dlsnode.storage.FileSystemLayout;
    import dlsnode.util.aio.JobNotification;
    import dlsnode.util.aio.EventFDJobNotification;

    /**************************************************************************

        CheckpointFile instance to provide on-disk representation of the
        checkpoint log.

    **************************************************************************/

    private CheckpointFile checkpoint_file;

    /**************************************************************************

        File paths for temporary and commited checkpoint file. During the
        writting, service will write into temporary path, and after being
        completely written and fsynced, service will call rename(2), atomically
        moving the checkpoint file to the new location.

     ************************************************************************/

    private FilePath tmp_file_path, file_path;


    /**************************************************************************

        Since CheckpointService will perform blocking operations (i.e. fsync)
        in commit() routine called from the timer callback, it needs to block
        only a fiber.

    **************************************************************************/

    private SelectFiber timer_fiber;

    /**************************************************************************

        JobNotification used for doing fiber-blocking IO calls.

    **************************************************************************/

    private JobNotification fiber_suspended_job;

    /**************************************************************************

        Timer suspended_job to periodically flush commit data to disk.

    **************************************************************************/

    private FiberTimerEvent checkpoint_timer;

    /**************************************************************************

        Number of seconds between each commit

    **************************************************************************/

    private ulong commit_seconds;

    /**************************************************************************

        Indicator if the service is being or is shutdown.

    **************************************************************************/

    private bool shutting_down;


    /**************************************************************************

        Constructor. Construct the CheckpointFile service.

        Params:
            dir = directory where the checkpoint file is stored.
            name = name of the checkpoint log file.

    *************************************************************************/

    public this ( cstring dir, cstring file_name )
    {
        this.tmp_file_path = new FilePath(FilePath.join(dir, file_name ~ ".tmp"));
        this.file_path = new FilePath(FilePath.join(dir, file_name));
        this.checkpoint_file = new CheckpointFile(dir, this.tmp_file_path.file);
    }

    unittest
    {
        auto dir = StringC.toDString(mkdtemp("/tmp/Dserviceunittest-XXXXXX\0".dup.ptr));
        auto service = new CheckpointService(dir, "file_name");
        testNoAlloc(service.renameCheckpointFile());
    }

    /**************************************************************************

        In-memory book keeping service of checkpoints for channel buckets.

    **************************************************************************/

    private struct ChannelBuckets
    {
        /**********************************************************************

            Checkpoint for a bucket.

        **********************************************************************/

        struct BucketCheckpoint
        {
            /*****************************************************************

                First hash that bucket can contain. Used to identify the bucket.

            *****************************************************************/

            public ulong bucket_start;

            /*****************************************************************

                Last write position reported by writers that's been synced
                to disk.

            ******************************************************************/

            public ulong bucket_offset;

            /*****************************************************************

                Indicator if the bucket slot is "occupied" with bucket
                checkpoint.

            *****************************************************************/

            public bool is_valid;

            /*****************************************************************

                Indicator if the bucket is still open.

            *****************************************************************/

            public bool is_open;

            /****************************************************************

              Routine syncing this bucket to disk

            ***************************************************************/

            public void delegate(JobNotification) fsync;

            /***************************************************************

                Name of the channel this bucket belong to.

            ****************************************************************/

            private mstring channel_name;
        }

        /**********************************************************************

            List of tracked buckets for this channel.

        **********************************************************************/

        private BucketCheckpoint[] buckets;


        /*********************************************************************

            Finds the bucket specified with the bucket_start
            for the given channel.

            Params:
                bucket_start = first hash this bucket can hold

            Return:
                pointer to the bucket checkpoint, or null if that bucket has
                not been tracked.

            Note:
                since per channel there can be only small number of open
                buckets, linear search is fine for this structure.

        **********************************************************************/

        public BucketCheckpoint* findBucket (ulong bucket_start)
        {
            foreach (i, bucket; this.buckets)
            {
                if (bucket.bucket_start == bucket_start && bucket.is_valid)
                {
                    return &this.buckets[i];
                }
            }

            return null;
        }

        /*********************************************************************

            Finds the bucket specified with the bucket_start
            for the given channel. If there's no such checkpoint, new one
            is created.

            Params:
                channel_name = name of the channel this bucket belongs to
                bucket_start = first hash this bucket can hold

            Return:
                pointer to the bucket checkpoint

        **********************************************************************/

        public BucketCheckpoint* getOrCreateBucket (cstring channel_name, ulong bucket_start)
        {
            size_t empty_slot_index = 0;
            bool empty_slot_found = false;

            // Try to search for the existing bucket, or for a slot
            // to be reused
            foreach (i, bucket; this.buckets)
            {
                if (bucket.bucket_start == bucket_start && bucket.is_valid)
                {
                    return &this.buckets[i];
                }

                if (bucket.is_valid == false)
                {
                    empty_slot_found = true;
                    empty_slot_index = i;
                }
            }

            // If there are no entries, allocate the first one:
            if (!empty_slot_found)
            {
                this.buckets ~= BucketCheckpoint();
                empty_slot_index = this.buckets.length - 1;
            }

            this.buckets[empty_slot_index].bucket_start = bucket_start;
            this.buckets[empty_slot_index].is_valid = true;
            oceanArray.copy(this.buckets[empty_slot_index].channel_name,
                    channel_name);
            return &this.buckets[empty_slot_index];
        }
    }

    /**************************************************************************

        Map of the bucket checkpoint list for every channel.

    **************************************************************************/

    private ChannelBuckets*[cstring] channels;


    /*************************************************************************

        List of the buckets that were about to be commited while the commit()
        cycle was running. At the end of the commit() operation, that fiber
        will commit these as well.

    *************************************************************************/

    private ChannelBuckets.BucketCheckpoint*[] to_commit;


    /*************************************************************************

        List of all channels to be commited (allows channels to be added
        during commit, not breaking iteration).

    *************************************************************************/

    private mstring[] channel_names;

    /*************************************************************************

        Indicates if the another commit is in progress.

    ************************************************************************/

    private bool commit_in_progress;


    /*************************************************************************

        Logs the position of the file for the given
        channel.

        Params:
            channel_name = name of the channel
            bucket_start = first hash that bucket can contain
            position = last position that was fsynced to disk

    **************************************************************************/

    public void checkpoint (cstring channel_name, ulong bucket_start, size_t position)
    {
        auto bucket_checkpoint = channels[channel_name].findBucket(bucket_start);
        assert(bucket_checkpoint, "Bucket checkpoint not found, probably bucketOpen is missing");

        bucket_checkpoint.bucket_offset = position;
        .log.trace("Just did a checkpoint: {} {} {}", channel_name, bucket_start, position);
    }



    /**************************************************************************

        Registers bucket that was just open. This immediately commits checkpoint
        log to disk and enables tracking of checkpoints for this bucket.

        Params:
            channel_name = name of the channel
            bucket_start = first hash that bucket can contain
            position = position of the file cursor (can be non-zero for non-empty
                            buckets).
            bucket_fsync = routine needs to be called to fsync this bucket to disk

    ***************************************************************************/

    public void bucketOpen (cstring channel_name, ulong bucket_start, size_t position,
            scope void delegate(JobNotification) bucket_fsync)
    {
        if (!(channel_name in this.channels))
        {
            this.channels[channel_name] = new ChannelBuckets();
        }

        if (auto bucket_checkpoint = channels[channel_name].getOrCreateBucket(channel_name,
                    bucket_start))
        {
            bucket_checkpoint.bucket_offset = position;
            bucket_checkpoint.bucket_start = bucket_start;
            bucket_checkpoint.is_open = true;
            bucket_checkpoint.fsync = bucket_fsync;

            .log.trace("I have just opened the bucket: {} {} {}", channel_name,
                    bucket_start, position);
        }
    }

    /**************************************************************************

        Informs the checkpoint service that the bucket was just closed. This
        tells checkpoint service to remove the bucket from bookkeeping engine
        as soon as it is commited to disk next time.

        Params:
            channel_name = name of the channel
            bucket_start = first hash that bucket can contain
            position = last position that was fsynced to disk

        Returns:
            true if the bucket should be closed, false if the bucket was
            already closed and no further closing is necessary.

    ***************************************************************************/

    public bool bucketClose (cstring channel_name, ulong bucket_start)
    {
        auto bucket_checkpoint = channels[channel_name].findBucket(bucket_start);

        if (bucket_checkpoint is null)
        {
            // Already fsynced and closed the bucket
            return false;
        }

        bucket_checkpoint.is_open = false;

        return true;
    }


    /**************************************************************************

        Starts the periodic commit of the checkpointed values to
        the commit  log

        Params:
            epoll = EpollSelectDispatcher instance
            commit_seconds = number of seconds between each commit()

    **************************************************************************/

    public void startService (EpollSelectDispatcher epoll, ulong commit_seconds)
    {
        this.shutting_down = false;

        auto fiber_stack_size = 4096 * 4;
        this.timer_fiber = new SelectFiber(epoll, &this.periodic_commit,
                fiber_stack_size);
        this.checkpoint_timer = new FiberTimerEvent(this.timer_fiber);
        this.fiber_suspended_job =
            new EventFDJobNotification(this.timer_fiber);
        this.commit_seconds = commit_seconds;
        this.timer_fiber.start();
    }


    /**************************************************************************

        Stops the checkpoint service, deleting the checkpoint file.
        Should be used on a normal shutdown.

        Note: It is user responsibility that make sure that all files that were
        tracked are flushed to disk prior to calling this method.

        Params:
            epoll = EpollSelectDispatcher instance

    **************************************************************************/

    public void stopService ( EpollSelectDispatcher epoll )
    {
        this.shutting_down = true;
        epoll.unregister(this.checkpoint_timer);

        if (file_path.exists)
        {
            OceanPath.remove(file_path.cString);
        }
    }


    /**************************************************************************

        Commits the checkpoint service state to disk.

        Params:
            suspended_job = suspended_job to block
                the current fiber on, in case of blocking fsync operation

        Returns:
            false if another commit is running, true if the commit was
            succesfull. In that case, caller should add desired entry to
            this.to_commit list in order for the entry to be flushed at
            the end of the current commit cycle.

    ***************************************************************************/

    private bool commit (JobNotification suspended_job)
    {
        if (this.shutting_down || this.commit_in_progress)
        {
            return false;
        }

        this.commit_in_progress = true;
        scope (exit)
        {
            this.commit_in_progress = false;
        }

        // Reset commit state
        this.to_commit.length = 0;

        // Copy the list of channels to commit:
        // This is done because, as the fiber will yield
        // during the iteration, we don't depend on the iteration
        // that might be invalidated during commit cycle
        // by other fibers doing bucketOpen and bucketClose
        // char[][]
        this.channel_names.length = this.channels.length;

        // workaround to a fact that this.channels.keys always allocate
        size_t i = 0;
        foreach (channel_name, bucket; this.channels)
        {
            this.channel_names[i].length = channel_name.length;
            this.channel_names[i][0..$] = channel_name[0..$];
            i++;
        }

        this.checkpoint_file.open();

        scope (failure)
        {
            // Don't leak file descriptors on failure to write
            // commit log
            this.checkpoint_file.close();

            // Remove the tmp file
            if (this.tmp_file_path.exists)
            {
                OceanPath.remove(this.tmp_file_path.cString);
            }
        }

        scope (success)
        {
            // On success, close and rename this file
            // to the permanent location
            this.checkpoint_file.close();

            if (!this.shutting_down)
            {
                this.renameCheckpointFile();
            }
            else
            {
                if (this.tmp_file_path.exists)
                {
                    OceanPath.remove(this.tmp_file_path.cString);
                }
            }
        }

        // layout of the file is like following
        // channel_name bucket_start position
        // channel_name bucket_start position
        // ...
        this.checkpoint_file.writeLines(
            ( void delegate ( cstring name, size_t bucket_start, size_t bucket_offset ) writeln )
            {
                .log.trace("About to iterate over channels");
                foreach (channel_name; this.channel_names)
                {
                    // The number of the buckets will not go down, we can
                    // commit at least the number of the buckets we see now
                    auto number_of_buckets = this.channels[channel_name].buckets.length;

                    for (auto i = 0; i < number_of_buckets; i++)
                    {
                        ChannelBuckets.BucketCheckpoint* bucket =
                            &(this.channels[channel_name].buckets[i]);

                        if (bucket.is_valid)
                        {
                            auto bucket_start = bucket.bucket_start;
                            auto bucket_offset = bucket.bucket_offset;

                            // If the file is still open, do fsync to make
                            // sure that at least the data stored in the
                            // checkpoint is indeed on the disk
                            if (bucket.is_open)
                            {
                                // It can happen that fsync is registered,
                                // but the bucket is actually closed before
                                // the job has been performed. In this case,
                                // it's ok not to fsync, since the bucket will
                                // always be fsynced and then closed
                                try
                                {
                                    bucket.fsync(suspended_job);
                                }
                                catch (ErrnoException e)
                                {
                                    // Ignore bad file descriptor suspended_jobs,
                                    // where the file is already closed
                                    if (e.errorNumber() != EBADF)
                                    {
                                        throw e;
                                    }
                                }
                            }

                            writeln (channel_name, bucket_start, bucket_offset);

                            // if the bucket was closed, we'll going to unregister it from
                            // checkpoint file.
                            if (!bucket.is_open)
                            {
                                bucket.is_valid = false;
                            }
                        }
                    }
                }

                // Commit the rest of the buckets that were opened during this iteration.
                // Note that these doesn't need to be fsynced to drive as they are either
                // 0-length or already had length at the openning time.
                // This makes things much simpler, as the fiber will not yield in the middle
                // of the operation.
                foreach (bucket; this.to_commit)
                {
                    if (bucket.is_valid)
                    {
                        writeln (bucket.channel_name,
                                bucket.bucket_start, bucket.bucket_offset);
                    }
                }
            }
        );

        return true;
    }

    /**************************************************************************

        Fiber routine performing periodic commit every commit_seconds.

    **************************************************************************/

    private void periodic_commit ()
    {
        while (true)
        {
            try
            {
                this.checkpoint_timer.wait(cast(double)this.commit_seconds);
                this.commit(this.fiber_suspended_job);
            }
            catch (Exception e)
            {
                .log.error("Exception in checkkpoint_delegate: {}", e.message());
            }
        }
    }

    /**************************************************************************

        Atomically renames checkpoint file from temporary location to the
        well-known name.

    **************************************************************************/

    private void renameCheckpointFile ( )
    {
        rename(tmp_file_path.cString.ptr, file_path.cString.ptr);
    }
}

/// unittest
unittest
{
    auto dir = StringC.toDString(mkdtemp("/tmp/Dserviceunittest-XXXXXX\0".dup.ptr));
    istring[3] channel_names = ["test1", "test2", "test3"];
    CheckpointService service = new CheckpointService (dir, "checkpoint.dat");

    // Dummy method for fsyncing the bucket
    void dummy_fsync (JobNotification ev)
    {
    }

    foreach (i, name; channel_names)
    {
        service.bucketOpen(name, i * 1000, 0, &dummy_fsync);
    }

    foreach (i, name; channel_names)
    {
        service.checkpoint(name, i * 1000, i * 2000);
    }

    foreach (i, name; channel_names)
    {
        service.bucketClose(name, i * 1000);
    }

    foreach (i, name; channel_names)
    {
        auto buckets = service.channels[name];

        auto checkpoint = buckets.findBucket(i * 1000);
        test!("!is")(checkpoint, null);

        test!("==")(checkpoint.bucket_offset, i * 2000);
    }

    service.commit(null);

    foreach (i, name; channel_names)
    {
        service.bucketOpen(name, (i + 1) * 1000, 0, &dummy_fsync);
    }

    // it should no longer be there, since the stuff got commited
    // and the buckets were closed
    foreach (i, name; channel_names)
    {
        auto buckets = service.channels[name];
        auto checkpoint = buckets.findBucket(i * 1000);
        test!("is")(checkpoint, null);
    }
}

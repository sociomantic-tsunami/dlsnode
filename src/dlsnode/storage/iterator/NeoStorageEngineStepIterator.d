/*******************************************************************************

    Neo StorageEngineStep iterator. Unlike legacy iterator, this iterator doesn't
    block the request. Instead it tells the request that the request should be
    put to sleep, waiting for the storage engine, and, when woken up, it should
    call the iterator again, which will then provide the requested value.

    A step iterator is distinguished from an opApply style iterator in that it
    has explicit methods to get the current key / value, and to advance the
    iterator to the next key. This type of iterator is essential for an
    asynchronous storage engine, as multiple iterations could be occurring in
    parallel (asynchronously), and each one needs to be able to remember its own
    state (ie which record it's up to, and which is next).

    copyright:
        Copyright (c) 2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.iterator.NeoStorageEngineStepIterator;

import dlsnode.storage.StorageEngine;

import dlsnode.util.aio.AsyncIO;
import dlsnode.util.aio.JobNotification;
import core.stdc.time;
import dlsnode.storage.util.Promise;

/*******************************************************************************

    DLS storage engine iterator for the neo protocol

*******************************************************************************/

public class NeoStorageEngineStepIterator
{
    import dlsnode.storage.FileSystemLayout : FileSystemLayout;
    import dlsnode.storage.BucketFile;
    import dlsnode.storage.Record;
    import ocean.core.array.Mutation: copy;

    import ocean.core.Buffer;
    import ocean.io.device.File;
    import ocean.meta.types.Qualifiers : mstring;

    /***************************************************************************

        Reference to storage engine, set by setStorage() method. The instance
        is set by a method, rather than the constructor, because an instance of
        this class can be constructed by one StorageEngine, and then
        re-used by others.

    ***************************************************************************/

    private StorageEngine storage;

    /***************************************************************************

        Record header's future.

    ***************************************************************************/

    private Future!(RecordHeader) header_future;

    /***************************************************************************

        Record value's future.

    ***************************************************************************/

    private Future!(void[]) value_future;

    /***************************************************************************

        Path of currently open bucket file.

    ***************************************************************************/

    private mstring bucket_path;

    /****************************************************************************

        Data buffer used for the buffered input.

    ****************************************************************************/

    private Buffer!(void) file_buffer;

    /***************************************************************************

        Current record value. As the log file's read position is
        advanced to the start of the next record, the length of the
        value buffer is set to 0. When the value() method is called, the
        buffer is then filled with the value of the current record. The
        value is only written once into this buffer per record, and the
        value() method will then simply return the contents of the
        buffer.

    ***************************************************************************/

    private mstring value_buffer;

    /***************************************************************************

        Minimum key to iterate over.

    ***************************************************************************/

    private time_t min_timestamp;

    /***************************************************************************

        Maximum key to iterate over.

    ***************************************************************************/

    private time_t max_timestamp;

    /***************************************************************************

        Hash of first record in the current bucket. Used by the
        FileSystemLayout.getNextBucket() method.

    ***************************************************************************/

    private time_t current_bucket_start;

    /***************************************************************************

        BucketFile instance.

    ***************************************************************************/

    private BucketFile file;

    /***************************************************************************

        Size of the file buffer to use

    ***************************************************************************/

    private size_t file_buffer_size;

    /***************************************************************************

        Internal state of this iterator. After initialization iterator cyclicly
        goes through three phases - looking for next bucket, where it will try
        to open the next bucket, expecting the next record header and expecting
        the next record value.

    ***************************************************************************/

    private enum State
    {
        /// Initializing the iterator
        Initializing,
        /// The previous bucket file has been read, now looking for the next one
        LookingForNextBucket,
        /// The opening of the next bucket is in progress in the background
        OpeningNextBucket,
        /// Expecting the record header
        ExpectingRecordHeader,
        /// Expecting the record value
        ExpectingRecordValue,
    }

    /// ditto
    private State state;

    /***************************************************************************

        Last read record header. Stored as class member in the case when the
        record key & value can't be read atomically.

    ***************************************************************************/

    private RecordHeader record_header;

    /***************************************************************************

        Constructor.

        Params:
            async_io = AsyncIO instance
            file_buffer_size = size of the buffer used for buffering the reads
                0 indicates no buffering.

    ***************************************************************************/

    public this ( AsyncIO async_io, size_t file_buffer_size )
    {
        assert(async_io);
        this.file_buffer_size = file_buffer_size;
        this.file_buffer.length(file_buffer_size);
        this.file = new BucketFile(async_io);
    }

    /***************************************************************************

        Initialises the iterator to iterate over all records in the
        storage engine within the specified range of keys. The first key
        in the specified range is queued up, ready to be fetched with
        the methods below.

        Params:
            channel = channel to iterate over
            min = timestamp of the first record to iterate
            max = timestamp of the last record to iterate

    ***************************************************************************/

    public void getRange ( StorageEngine channel, time_t min, time_t max )
    {
        this.reset(channel, min, max);
    }

    /***************************************************************************

        Result of the next method.

    ***************************************************************************/

    enum NextResult
    {
        /// No more data in the bucket file
        NoMoreData,
        /// The caller should wait for more data
        WaitForData,
        /// The record has loaded in the provided buffer
        RecordRead,
    }

    /***************************************************************************

        Advances the iterator to the next record or to the first record in
        the storage engine, if this.started is false.

        Params:
            job_notification = request handler which will resume the
                request once the data is available, indicating that the call
                to this method should be repeated
            key = read key of the record
            value = read value of the record

        Returns:
            NextResult indicating if there are no more data in the bucket,
            if the caller should wait for the read to complete, or if the
            next record is read.

    ***************************************************************************/

    public NextResult next (JobNotification job_notification,
            out time_t key,
            ref void[] value )
    {
        bool end_of_bucket, end_of_channel;

        // State machine. Start from the last state and move through
        // the records.
        try do
        {
            switch (this.state)
            {
                case State.Initializing:
                    bool wait_for_open;
                    auto has_more_buckets = this.openFirstBucket(job_notification,
                            wait_for_open);

                    if (wait_for_open)
                    {
                        return NextResult.WaitForData;
                    }

                    if (!has_more_buckets)
                        return NextResult.NoMoreData;

                    this.state = State.ExpectingRecordHeader;
                    break;

                case State.LookingForNextBucket:
                case State.OpeningNextBucket:
                    bool wait_for_open;
                    auto has_more_buckets = this.openNextBucket(job_notification,
                            wait_for_open);

                    if (wait_for_open && has_more_buckets)
                    {
                        this.state = State.OpeningNextBucket;
                        return NextResult.WaitForData;
                    }

                    if (!has_more_buckets)
                        return NextResult.NoMoreData;

                    this.state = State.ExpectingRecordHeader;
                    break;

                case State.ExpectingRecordHeader:
                    if (!this.header_future.valid())
                    {
                        this.header_future = this.file.nextRecord(job_notification);

                        if (!this.header_future.valid())
                        {
                            return NextResult.WaitForData;
                        }
                    }

                    if ( this.header_future.error() )
                    {
                        this.file.close();
                        this.state = State.LookingForNextBucket;
                    }
                    else
                    {
                        // We have the record, let's read value
                        try
                        {
                            this.record_header = this.header_future.get();
                            this.state = State.ExpectingRecordValue;
                        }
                        catch (Exception e)
                        {
                            // on error reading, skip this bucket
                            this.state = State.LookingForNextBucket;
                        }
                    }
                    break;

                case state.ExpectingRecordValue:
                    if (!this.value_future.valid())
                    {
                        this.value_future = this.file.readRecordValue(job_notification,
                                this.record_header);

                        if (!this.value_future.valid())
                        {
                            return NextResult.WaitForData;
                        }
                    }

                    if ( this.header_future.error() )
                    {
                        // Something got bad reading the last value,
                        // move to the next bucket
                        this.file.close();
                        this.state = State.LookingForNextBucket;
                    }
                    else
                    {
                        try
                        {
                            key = cast(time_t)this.record_header.key;
                            value.copy(this.value_future.get());
                            this.state = State.ExpectingRecordHeader;
                            return NextResult.RecordRead;
                        }
                        catch (Exception e)
                        {
                            // on error reading, skip this bucket
                            this.state = State.LookingForNextBucket;
                        }
                    }
                    break;

                default:
                    assert(false);
            }
        }
        while ( true );
        catch (Exception e)
        {
            // just silently ignore possible data errors (the storage layer
            // will already report them)
            return NextResult.NoMoreData;
        }

        assert(false);
    }

    /***************************************************************************

        Search for and open the first bucket in the range.

        Params:
            job_notification = notification to notify the user on the bucket
            opening completion
            wait_for_open = indicator if the user needs to wait for the open

        Returns:
            false if the end of channel/range has been reached and no more
            data is available, true otherwise.

    ***************************************************************************/

    private bool openFirstBucket ( JobNotification job_notification, out bool wait_for_open)
    {
        return openBucket(()
                {
                    hash_t tmp;
                    auto ret = FileSystemLayout.getFirstBucketInRange(
                            this.storage.working_dir,
                            this.bucket_path, tmp,
                            this.min_timestamp, this.max_timestamp);

                    this.current_bucket_start = tmp;
                    return ret;
                }, job_notification, wait_for_open);
    }

    /***************************************************************************

        Search for and open the next bucket in the range.

        Params:
            job_notification = notification to notify the user on the bucket
            opening completion
            wait_for_open = indicator if the user needs to wait for the open

        Returns:
            false if the end of channel/range has been reached and no more
            data is available, true otherwise.

    ***************************************************************************/

    private bool openNextBucket (JobNotification job_notification, out bool wait_for_open)
    {
        // hash_t to satisfy the interface of FileSystemLayout
        hash_t next_bucket_start;

        auto end_of_channel = !openBucket(()
                {
                     /* move to the next bucket only if we're not resuming the
                        opening of the current one. */
                     return this.state != State.OpeningNextBucket?
                         FileSystemLayout.getNextBucket(
                                this.storage.working_dir,
                                this.bucket_path, next_bucket_start,
                                this.current_bucket_start, this.max_timestamp):
                         false;
                }, job_notification, wait_for_open);


        if (!end_of_channel && this.state != State.OpeningNextBucket)
        {
            this.current_bucket_start = next_bucket_start;
        }

        return !end_of_channel;
    }

    /***************************************************************************

        Searche for and open the bucket for iterating.

        Params:
            find_bucket = delegate called to find the next bucket's path.
            job_notification = notification to notify the user on the bucket
            opening completion
            wait_for_open = indicator if the user needs to wait for the open

        Returns:
            false if the end of channel/range has been reached and no more
            data is available, true otherwise.

    ***************************************************************************/

    private bool openBucket (scope bool delegate() find_bucket,
            JobNotification job_notification,
            out bool wait_for_open)
    {
        auto end_of_channel = find_bucket();

        // Clear the iterator's cursor
        this.resetCursorState();

        if (end_of_channel)
        {
            // Nothing to do, no buckets.
            return false;
        }

        auto opened = this.file.openAsync(this.bucket_path, job_notification,
                this.file_buffer[], File.ReadExisting);

        wait_for_open = !opened;

        assert(!end_of_channel);
        return true;
    }

    /***************************************************************************

        Resets all class members to their initial state.

        Params:
            storage = storage engine to iterate over
            min = minimum timestamp to iterate over
            max = maximum timestamp to iterate over

    ***************************************************************************/

    private void reset ( StorageEngine storage, time_t min, time_t max )
    {
        this.storage = storage;
        this.min_timestamp = min;
        this.max_timestamp = max;

        this.bucket_path.length = 0;

        this.current_bucket_start = this.min_timestamp;

        this.resetCursorState();
        this.state = State.Initializing;

        if (this.file.is_open)
        {
            this.file.close();
        }

        this.file_buffer.reset();
        this.file_buffer.length(file_buffer_size);

        // Flush all the output buffers for the files in this
        // channel - ensures that this iterator read over as fresh
        // as possible set of data (important in tests, for example,
        // where the exact number of returned records is crucial)
        this.storage.flushData();
    }


    /***************************************************************************

        Resets all members relating to the reading of a record, ready to
        read the next record from the log file.

    ***************************************************************************/

    private void resetCursorState ( )
    {
        this.value_buffer.length = 0;
    }
}

/*******************************************************************************

    Abstract base class for DLS node step-by-step iterators. An iterator
    class must be implemented for each storage engine.

    A step iterator is distinguished from an opApply style iterator in that it
    has explicit methods to get the current key / value, and to advance the
    iterator to the next key. This type of iterator is essential for an
    asynchronous storage engine, as multiple iterations could be occurring in
    parallel (asynchronously), and each one needs to be able to remember its own
    state (ie which record it's up to, and which is next). This class provides
    the interface for that kind of iterator.

    This abstract iterator class has no methods to begin an iteration. As
    various different types of iteration are possible, it is left to derived
    classes to implement suitable methods to start iterations.

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.iterator.StorageEngineStepIterator;


/*******************************************************************************

    Imports

*******************************************************************************/

import dlsnode.storage.iterator.model.IStorageEngineStepIterator;
import dlsnode.storage.StorageEngine;

import dlsnode.util.aio.AsyncIO;
import dlsnode.util.aio.JobNotification;
import core.stdc.time;

/*******************************************************************************

    DLS storage engine iterator

*******************************************************************************/

public class StorageEngineStepIterator: IStorageEngineStepIterator
{
    import dlsnode.storage.FileSystemLayout : FileSystemLayout;
    import dlsnode.storage.BucketFile;
    import dlsnode.storage.Record;

    import ocean.core.Buffer;
    import ocean.io.device.File;
    import ocean.meta.types.Qualifiers : cstring, mstring;

    import Hash = swarm.util.Hash;


    /***************************************************************************

        Reference to storage engine, set by setStorage() method. The instance
        is set by a method, rather than the constructor, because an instance of
        this class can be constructed by one StorageEngine, and then
        re-used by others.

    ***************************************************************************/

    private StorageEngine storage;


    /***************************************************************************

        Indicates if iteration has already started. If next() is called when
        this value is false, the iteration will be started.

    ***************************************************************************/

    private bool started;


    /***************************************************************************

        Iteration abort flag. Set by abort(), causes lastKey() to return true.

    ***************************************************************************/

    private bool aborted;


    /***************************************************************************

        Path of currently open bucket file.

    ***************************************************************************/

    private mstring bucket_path;


    /***************************************************************************

        Header of current record. When a record is finished with, this
        value is reset to FileSystemLayout.RecordHeader.init.

    ***************************************************************************/

    private RecordHeader current_header;


    /***************************************************************************

        Indicates whether the header of the current record has been read
        or not.

    ***************************************************************************/

    private bool read_header;


    /****************************************************************************

        Data buffer used for the buffered input.

    ****************************************************************************/

    private Buffer!(void) file_buffer;

    /***************************************************************************

        Current record key. As the log file's read position is advanced
        to the start of the next record, the length of the key buffer is
        set to 0. When the key() method is called, the buffer is then
        filled with the key of the current record. The key is only
        written once into this buffer per record, and the key() method
        will then simply return the contents of the buffer.

    ***************************************************************************/

    private mstring key_buffer;


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

        Minimum and maximum keys to iterate over.

    ***************************************************************************/

    private hash_t min_hash;

    private hash_t max_hash;


    /***************************************************************************

        Hash of first record in the current bucket. Used by the
        FileSystemLayout.getNextBucket() method.

    ***************************************************************************/

    private hash_t current_bucket_start;


    /***************************************************************************

        BucketFile instance.

    ***************************************************************************/

    private BucketFile file;

    /***************************************************************************

        Size of the file buffer to use

    ***************************************************************************/

    private size_t file_buffer_size;

    /***************************************************************************

        Constructor.

        Params:
            storage = storage engine to iterate over
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

        Storage initialiser.

        Params:
            storage = storage engine to iterate over

    ***************************************************************************/

    public void setStorage ( StorageEngine storage )
    {
        this.storage = storage;
    }


    /***************************************************************************

        Initialises the iterator to iterate over all records in the
        storage engine. The first key is queued up, ready to be fetched
        with the methods below.

        Params:
            suspended_job = JobNotification instance to block the caller on.

    ***************************************************************************/

    public override void getAll ( JobNotification suspended_job )
    in
    {
        assert(this.storage, typeof(this).stringof ~ ".getAll: storage not set");
    }
    do
    {
        this.reset(suspended_job, hash_t.min, hash_t.max);
    }


    /***************************************************************************

        Initialises the iterator to iterate over all records in the
        storage engine within the specified range of keys. The first key
        in the specified range is queued up, ready to be fetched with
        the methods below.

        Params:
            suspended_job = JobNotification instance to block the caller on.
            min = string containing the hexadecimal key of the first
                record to iterate
            max = string containing the hexadecimal key of the last
                record to iterate

    ***************************************************************************/

    public override void getRange ( JobNotification suspended_job, cstring min, cstring max )
    in
    {
        assert(this.storage, typeof(this).stringof ~ ".getRange: storage not set");
    }
    do
    {
        this.reset(suspended_job,
                Hash.straightToHash(min), Hash.straightToHash(max));
        this.started = false;
    }

    /***************************************************************************

        Gets the string containing key of the current record the iterator is
        pointing to.

        Returns:
            current key

    ***************************************************************************/

    public override cstring key ( )
    {
        if ( this.read_header && !this.key_buffer.length )
        {
            this.key_buffer.length = Hash.HexDigest.length;
            Hash.toHexString(this.current_header.key, this.key_buffer);
        }

        return this.key_buffer;
    }


    /***************************************************************************

        Gets the key of the current record the iterator is pointing to.

        Returns:
            current key

    ***************************************************************************/

    public time_t key_timestamp ()
    {
        return cast(time_t)this.current_header.key;
    }

    /***************************************************************************

        Gets the value of the current record the iterator is pointing
        to.

        Params:
            suspended_job = JobNotification to block the fiber on until read is completed

        Returns:
            current value

    ***************************************************************************/

    public override cstring value ( JobNotification suspended_job )
    {
        if ( this.read_header && !this.value_buffer.length )
        {
            this.file.readRecordValue(suspended_job,
                    this.current_header, this.value_buffer);
        }

        return this.value_buffer;
    }


    /***************************************************************************

        Advances the iterator to the next record or to the first record in
        the storage engine, if this.started is false.

        Params:
            suspended_job = JobNotification to block the fiber on until read is completed

    ***************************************************************************/

    public override void next ( JobNotification suspended_job )
    in
    {
        assert(this.storage, typeof(this).stringof ~ ".next: storage not set");
    }
    do
    {
        if ( !this.started )
        {
            this.started = true;
            this.getFirstRecord(suspended_job);
            return;
        }

        bool end_of_bucket, end_of_channel;

        // In case cursor is positioned at the end of the bucket,
        // there could be no more records in it (this.file.getNextRecord
        // would return `true`) and we need to skip to the next bucket.
        // This is why we are using loop here - we will loop through
        // all buckets until we either find a record, or we hit end of
        // the channel.
        do
        {
            // If the last record's header was read, but the value was
            // not read, we need to seek the file's read position to
            // the start of the next record's header (this is usually
            // done when the record value is read).
            if ( this.read_header && !this.value_buffer.length )
            {
                this.file.skipRecordValue(suspended_job,
                        this.current_header);
            }

            this.resetCursorState();

            end_of_bucket = this.file.nextRecord(suspended_job,
                    this.current_header);

            if ( end_of_bucket )
            {
                this.file.close(suspended_job);

                hash_t next_bucket_start;
                end_of_channel = FileSystemLayout.getNextBucket(
                    this.storage.working_dir, this.bucket_path,
                    next_bucket_start,
                    this.current_bucket_start, this.max_hash);

                if ( !end_of_channel )
                {
                    this.current_bucket_start = next_bucket_start;
                    this.file.open(this.bucket_path, suspended_job,
                            this.file_buffer[],
                            File.ReadExisting);
                }
            }
            else
            {
                this.read_header = true;
            }
        }
        while ( end_of_bucket && !end_of_channel );

        if ( end_of_channel )
        {
            this.current_header = this.current_header.init;
        }
    }


    /***************************************************************************

        Tells whether the current record pointed to by the iterator is the last
        in the iteration.

        This method may be overridden, but the default definition of the
        iteration end is that the current key is empty.

        Returns:
            true if the current record is the last in the iteration

    ***************************************************************************/

    public override bool lastKey ( )
    {
        return this.key.length == 0 || this.aborted;
    }



    /***************************************************************************

        Aborts the current iteration. Causes lastKey() (the usual condition
        which is checked to indicate the end of the iteration) to always return
        true, until the iteration is restarted.

    ***************************************************************************/

    public override void abort ( )
    {
        this.aborted = true;
    }


    /***************************************************************************

        Gets the first record in the iteration. Called by the first call
        to next(), above.

        Params:
            suspended_job = JobNotification to block the fiber on until read is completed

    ***************************************************************************/

    private void getFirstRecord ( JobNotification suspended_job )
    {
        // Get the name of the first bucket file to scan. no_buckets is
        // set to true if no buckets exist in the specified range.
        bool no_buckets;
        if ( this.min_hash == hash_t.min && this.max_hash == hash_t.max )
        {
            no_buckets = FileSystemLayout.getFirstBucket(this.storage.working_dir,
                this.bucket_path, this.current_bucket_start);
        }
        else
        {
            no_buckets = FileSystemLayout.getFirstBucketInRange(
                this.storage.working_dir, this.bucket_path,
                this.current_bucket_start, this.min_hash, this.max_hash);
        }

        // Clear the iterator's cursor
        this.resetCursorState();

        if (no_buckets)
        {
            // Nothing to do, no buckets.
            return;
        }

        // Open the bucket and position the cursor to first record
        this.file.open(this.bucket_path,
                suspended_job, this.file_buffer[], File.ReadExisting);
        this.next(suspended_job);
    }


    /***************************************************************************

        Performs required de-initialisation behaviour - closes any open
        bucket file.

        Params:
            suspended_job = JobNotification instance to block the caller on.

    ***************************************************************************/

    public void finished ( JobNotification suspended_job )
    {
        this.reset(suspended_job, 0, 0);
    }


    /***************************************************************************

        Resets all class members to their initial state.

        Params:
            suspended_job = JobNotification instance to block the caller on.
            min = minimum hash to iterate over
            max = maximum hash to iterate over

    ***************************************************************************/

    private void reset ( JobNotification suspended_job, hash_t min, hash_t max )
    in
    {
        assert(this.storage, typeof(this).stringof ~ ".getAll - storage not set");
    }
    do
    {
        this.started = false;
        this.aborted = false;

        this.min_hash = min;
        this.max_hash = max;

        this.bucket_path.length = 0;

        this.current_bucket_start = 0;

        this.resetCursorState();

        if (this.file.is_open)
        {
            this.file.close(suspended_job);
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
        this.read_header = false;

        this.current_header = this.current_header.init;

        this.key_buffer.length = 0;
        this.value_buffer.length = 0;
    }
}

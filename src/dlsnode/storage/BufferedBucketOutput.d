/******************************************************************************

    Buffered Output implementation for DLS storage engine

    Implements the Put database command using wrapper around BufferedOutput.

    See dlsnode.storage.FileSystemLayout and dlsnode.storage.BucketFile for
    a description of the database file organization and slot/bucket association,
    respectively.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

 ******************************************************************************/

module dlsnode.storage.BufferedBucketOutput;

/******************************************************************************

    Imports

 ******************************************************************************/

import dlsnode.storage.BucketFile;
import dlsnode.storage.FileSystemLayout;
import dlsnode.storage.Record;

import Hash = swarm.util.Hash;

import ocean.core.Array : append, concat, copy;
import ocean.core.Exception;

import ocean.io.stream.Buffered;

import ocean.io.device.File;

import ocean.io.FilePath;

import dlsnode.util.aio.JobNotification;
import dlsnode.util.aio.AsyncIO;

import ocean.util.log.Logger;

/******************************************************************************

    Static module logger.

******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.storage.BufferedBucketOutput");
}



/******************************************************************************/

public class BufferedBucketOutput
{
    import dlsnode.storage.checkpoint.CheckpointService;
    import ocean.core.Buffer;
    import ocean.meta.types.Qualifiers : cstring, mstring;

    /**************************************************************************

        Default output buffer size: 64 kB

     **************************************************************************/

    static immutable size_t DefaultBufferSize = 64 * 1024;

    /**************************************************************************

        This alias for chainable methods

     **************************************************************************/

    alias typeof(this) This;

    /**************************************************************************

        Current slot and bucket, used to check when a record is being written to
        a different file.

     **************************************************************************/

    private SlotBucket current_sb;

    /**************************************************************************

        Bucket file open indicator: true indicates that a bucket file is
        currently open

     **************************************************************************/

    private bool file_open;

    /**************************************************************************

        Base directory name

     **************************************************************************/

    private mstring base_dir;

    /**************************************************************************

        Channel name that this writer currently belongs to.

    **************************************************************************/

    private mstring channel_name;

    /**************************************************************************

        Underlying File instance for the currently open file

     **************************************************************************/

    private BucketFile file;

    /**************************************************************************

        FilePath instance, used in openFile()

     **************************************************************************/

    private FilePath path;

    /**************************************************************************

        Slot path buffer, used in openFile()

     **************************************************************************/

    private mstring slot_path_str;

    /**************************************************************************

        Bucket path buffer, used in openFile()

     **************************************************************************/

    private mstring bucket_path_str;

    /**************************************************************************

        Output buffer

     **************************************************************************/

    private BufferedOutput output;

    /**************************************************************************

        Checkpointer service instance.

    **************************************************************************/

    private CheckpointService checkpointer;

    /**************************************************************************

        Constructor

        Params:
            buffer_size = output buffer size
            async_io = AsyncIO instance

        Throws:
            IOException if the size info file is invalid

     **************************************************************************/

    public this ( CheckpointService checkpointer,
           AsyncIO async_io, size_t buffer_size )
    {
        this.checkpointer = checkpointer;
        this.file = new BucketFile(async_io);
        this.output = new BufferedOutput(this.file, buffer_size, &this.flushNotify);

        this.path = new FilePath;
    }

    /*************************************************************************

        Indicator if the checkpoint needs to be made (if the file flushed).
        This can't be done directly in the flushNotify method, as
        BufferedOutput is not calling the delegate with the JobNotification
        from the currently running fiber.

    **************************************************************************/

    private bool need_checkpoint;

    /**************************************************************************

        BufferedOutput flush notifier. Gets notified by BufferedOutput when
        its contents is flushed and synced to disk. The writer will then
        write a checkpoint to the CheckpointService saying that that was a
        last known position to be synced to disk.

    **************************************************************************/

    private void flushNotify()
    {
        this.need_checkpoint = true;
    }

    /**************************************************************************

        Sets the channel's base directory (called when a storage engine instance
        is recycled and then re-used). The directory is passed on to the size
        info file class.

        Params:
            channel_name = name of the channel this writer currently belongs to
            dir = channel base directory

     **************************************************************************/

    public void setDir ( cstring channel_name, cstring dir )
    {
        this.channel_name.copy(channel_name);
        this.base_dir.copy(dir);
    }

    /**************************************************************************

        Appends a record to the bucket file that corresponds to key.

        Params:
            key = record key
            value = record value
            record_buffer = buffer used internally for rendering entire record
                            passing it to BufferedOutput.
            suspended_job = JobNotification to block
                the current fiber on while doing blocking IO
        Returns:
            this instance

     **************************************************************************/

    public This put ( hash_t key, cstring value, ref ubyte[] record_buffer,
            JobNotification suspended_job )
    {
        this.openFile(key, suspended_job);

        if (this.need_checkpoint)
        {
            this.checkpointer.checkpoint(this.channel_name, this.current_sb.firstKey(),
                    this.file.noncached_file_pos);
            this.need_checkpoint = false;
        }

        this.writeRecord(key, value, record_buffer);

        return this;
    }

    /***************************************************************************

        Returns:
            true if the file is currently open

    ***************************************************************************/

    public bool isOpen ( )
    {
        return this.file_open;
    }

    /**************************************************************************

        Flushes the write buffer, closes the current bucket file and writes the
        size info.

        Returns:
            this instance

     **************************************************************************/

    public This commitAndClose ()
    {
        if ( this.file_open )
        {
            this.closeFile();
        }

        return this;
    }

    /**************************************************************************

        Flushes the write buffer

     **************************************************************************/

    public void flushData ( )
    {
        this.output.flush();
    }


    /**************************************************************************

        Appends a record to the bucket file that corresponds to key.

        Params:
            key = record key
            value = record value
            record_buffer = buffer used internally for rendering entire record
                            passing it to BufferedOutput.

        Returns:
            this instance

     **************************************************************************/

    private void writeRecord ( hash_t key, cstring value, ref ubyte[] record_buffer )
    {
        this.file.writeRecord(this.output, key, value, record_buffer);
    }

    /**************************************************************************

        Opens the bucket file that corresponds to key or creates a new one if
        not existing. Sets the current slot/bucket corresponding to key.
        If a bucket file is currently open and the current slot/bucket
        correspond to key, nothing is done.

        Params:
            key = record key
            suspended_job = JobNotification to block
                the current fiber on while doing blocking IO

     **************************************************************************/

    private void openFile ( hash_t key,
            JobNotification suspended_job )
    {
        SlotBucket sb;

        sb.fromKey(key);

        // There could be the cache colision inside cache,
        // so we need to check if we really got the right one
        if (this.current_sb != sb || !this.file_open)
        {
            this.commitAndClose();

            FileSystemLayout.getBucketPathFromParts(this.base_dir,
                    this.slot_path_str,
                    this.bucket_path_str,
                    sb);

            this.path.set(this.slot_path_str);

            if (!this.path.exists)
            {
                this.path.create();
            }

            // Open bucket file. This doesn't perform any reading except
            // the buffer header, so don't use any buffering
            Buffer!(void) empty_buffer;

            // NOTE: passing null here as a JobNotification
            // makes the following call atomic. This is important
            // as it makes sure that the BucketFile can't be
            // in some intermediate state between open & closed
            this.file.open(this.bucket_path_str, null,
                    empty_buffer[], File.ReadWriteAppending);

            this.current_sb = sb;
            this.file_open = true;

            this.checkpointer.bucketOpen(this.channel_name,
                    this.current_sb.firstKey, this.file.file_length,
                    &this.file.sync);
        }
    }

    /**************************************************************************

        Closes the current bucket file after flushing the output buffer.

     **************************************************************************/

    private void closeFile ( )
    in
    {
        assert (this.file_open, typeof (this).stringof ~ ".closeFile: file not open");
    }
    body
    {
        // NOTE: it is very important that none of the following calls
        // could cause fiber yield. Because this file is reused inside LRU
        // cache, and closed when expired, so it's important this slot
        // is not allowed to enter some intermediate state.

        // Best-effort way to figure out that nothing can suspend a fiber
        // is to make sure that there's no SuspenableRequestFiber
        // passed to any of the following methods.

        this.output.flush();

        this.checkpointer.bucketClose(this.channel_name,
                this.current_sb.firstKey);

        this.file.close();

        this.file_open = false;
        this.need_checkpoint = false;
    }
}


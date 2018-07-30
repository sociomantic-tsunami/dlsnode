/******************************************************************************

    Implementation of the legacy storage protocol

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsnode.storage.protocol.StorageProtocolLegacy;

import ocean.transition;

import dlsnode.storage.protocol.model.IStorageProtocol;
import dlsnode.storage.util.Promise;
import ocean.io.device.File;
import ocean.util.log.Logger;
import dlsnode.util.aio.JobNotification;
import ocean.core.Verify;
import ocean.core.array.Mutation;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.storage.protocol.StorageProtocolLegacy");
}

/******************************************************************************

    Implements legacy storage layout. The records are stored one after another
    in the following fashion:

    ---------------------------------------------------------------
    | key | len |      value     | key | len |   value | key | ...
    ---------------------------------------------------------------
    \          /                 \          /
     \        /                   \        /
      \      /                     \      /
       header                       header

******************************************************************************/

scope class StorageProtocolLegacy: IStorageProtocol
{

    /**************************************************************************

        Tries to read the next record header from the file. In case the record
        can't be fetched, the request should suspend itself, wait to be resumed
        by job_notification and then collect results from the resulting future.

        Params:
            job_notification = JobNotification to wake up
                the fiber on the completion of the IO operation
            file = bucket file instance to read from

        Returns:
            future that either contains or will contain the next record's
            header.

    **************************************************************************/

    public override Future!(RecordHeader) nextRecord (
            JobNotification job_notification,
            BucketFile file)
    {
        return file.readDataAsync!(RecordHeader)(job_notification, RecordHeader.sizeof);
    }

    /**************************************************************************

        Tries to read the next record value from the file. In case the record
        can't be fetched, the request should suspend itself, wait to be resumed
        by job_notification and then collect results from the resulting future.

        Params:
            job_notification = JobNotification to wake up
                the fiber on the completion of the IO operation
            file = bucket file instance to read from
            header = current record's header

        Returns:
            future that either contains or will contain the record's value

    **************************************************************************/

    public override Future!(void[]) readRecordValue (
            JobNotification job_notification,
            BucketFile file, RecordHeader header)
    {
        // Read value from file
        return file.readDataAsync!(void[])(job_notification, header.len);
    }

    /**************************************************************************

        Reads next record header from the file, if any.

        Params:
            suspended_job = JobNotification to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = record header to fill

        Returns:
            false if the record was read, true otherwise.

    **************************************************************************/

    public override bool nextRecord (
            JobNotification suspended_job,
            BucketFile file, ref RecordHeader header )
    {
        if ( file.file_pos + header.sizeof >= file.file_length )
        {
            return true; // end of bucket file
        }

        // Read header of next record
        file.readData(suspended_job, (cast(void*)&header)[0..header.sizeof]);

        // Sanity check: if the length of the record is beyond the end of
        // the file, then just return. This can occur in two cases:
        //      1. A record being read as it is being written
        //      2. Corrupt data
        if ( file.file_pos + header.len > file.file_length )
        {
            return true; // end of bucket file
        }

        return false; // read header successfully
    }

    /**************************************************************************

        Reads the next record value from the file.

        Params:
            suspended_job = JobNotification to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = current record's header
            value = value buffer to read record value to

    **************************************************************************/

    public override void readRecordValue (
            JobNotification suspended_job,
            BucketFile file, RecordHeader header, ref mstring value )
    {
        // Read value from file
        value.length = header.len;
        file.readData(suspended_job, value);
    }

    /**************************************************************************

        Skips the next record value in the file.

        Params:
            suspended_job = JobNotification to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = current record's header

    **************************************************************************/

    public override void skipRecordValue (
            JobNotification suspended_job,
            BucketFile file, ref RecordHeader header )
    {
        file.seek(header.len, File.Anchor.Current);
    }

    /**************************************************************************

        Layouts the record to output buffer.

        Params:
            output = buffered output instance to write to
            key = record key
            value = record's value
            record_buffer = buffer used internally for rendering entire record
                            passing it to BufferedOutput.

    **************************************************************************/

    public override void writeRecord (BufferedOutput output,
            hash_t key, cstring value, ref ubyte[] record_buffer)
    {
        RecordHeader header;

        header.key = key;
        header.len = value.length;

        // Preformat the data before sending it to BufferedOutput to
        // avoid flushing out of record boundary
        record_buffer.length =
            header.sizeof + (value.length * typeof (value[0]).sizeof);

        record_buffer[0 .. header.sizeof] = (cast(ubyte*)&header)[0 .. header.sizeof];
        record_buffer[header.sizeof .. $] = (cast(ubyte*)value.ptr)[0 .. value.length];

        output.append(record_buffer.ptr, record_buffer.length);
    }
}
